using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumerWorker<T> : IQueueConsumerWorker where T : class
    {
        private readonly Subscription _subscription;
        private readonly IModel _model;
        private readonly ISerializer _serializer;
        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;
        private readonly IMessageRejectionHandler _messageRejectionHandler;
        private readonly Func<bool> _scaleCallbackFunc;

        public TimeSpan CheckAliveFrequency { get; set; }

        public bool ModelIsClosed => _model.IsClosed;

        public RabbitMQConsumerWorker(IConnection connection, string queueName, 
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler, 
            ISerializer serializer, Func<bool> scaleCallbackFunc)
        {
            _model = connection.CreateModel();
            _model.BasicQos(0, 1, false);
            _subscription = new Subscription(_model, queueName, false);
            _messageProcessingWorker = messageProcessingWorker;
            _messageRejectionHandler = messageRejectionHandler;
            _serializer = serializer;
            _scaleCallbackFunc = scaleCallbackFunc;
            CheckAliveFrequency = new TimeSpan(0, 0, 10);
        }

        public async Task DoConsumeAsync(CancellationToken cancellationToken)
        {
            //Iterate while thread hasn't been canceled
            while (!cancellationToken.IsCancellationRequested)
            {
                //Create BasicDeliverEventArgs object and start trying to get the next message on the queue
                var messageReceived = _subscription.Next(Convert.ToInt32(CheckAliveFrequency.TotalMilliseconds),
                    out BasicDeliverEventArgs lastResult);

                //If a message hasn't been succesfully fetched from the queue
                if (!messageReceived)
                {
                    //If the model has been closed
                    if (!_model.IsOpen)
                    {
                        //Dispose ConsumerWorker
                        Dispose();

                        //Throw AlreadyClosedException (model is already closed)
                        throw new global::RabbitMQ.Client.Exceptions.AlreadyClosedException(_model.CloseReason);
                    }
                }

                //If something was in fact returned from the queue
                if (lastResult != null)
                {
                    //Get message body
                    var messageBody = GetBody(lastResult);

                    //Create empty messageObject instance
                    var messageObject = default(T);

                    try
                    {
                        //Try to deserialize message body into messageObject
                        messageObject = _serializer.Deserialize<T>(messageBody);
                    }
                    catch (Exception exception)
                    {
                        //Create DeserializationException to pass to RejectionHandler
                        var deserializationException = new DeserializationException("Unable to deserialize data.", exception)
                        {
                            SerializedDataString = messageBody,
                            SerializedDataBinary = lastResult.Body,
                            QueueName = _subscription.QueueName
                        };
                        //Pass DeserializationException to RejectionHandler
                        _messageRejectionHandler.OnRejection(deserializationException);

                        //Remove message from queue after RejectionHandler dealt with it
                        Nack(lastResult.DeliveryTag, false);
                    }

                    //If message has been successfully deserialized and messageObject is populated
                    if (messageObject != null)
                    {
                        //Create messageFeedbackSender instance with corresponding model and deliveryTag
                        IMessageFeedbackSender messageFeedbackSender = new RabbitMQMessageFeedbackSender(_model, lastResult.DeliveryTag);

                        try
                        {
                            //Call given messageProcessingWorker's OnMessage method to proceed with message processing
                            await _messageProcessingWorker.OnMessageAsync(messageObject, messageFeedbackSender, 
                                cancellationToken).ConfigureAwait(false);

                            //If message has been processed with no errors but no Acknoledgement has been given
                            if (!messageFeedbackSender.MessageAcknoledged)
                            {
                                //Acknoledge message
                                _subscription.Ack();
                            }
                        }
                        catch (Exception)
                        {
                            //If something went wrong with message processing and message hasn't been acknoledged yet
                            if (!messageFeedbackSender.MessageAcknoledged)
                            {
                                //Negatively Acknoledge message, asking for requeue
                                Nack(lastResult.DeliveryTag, true);
                            }

                            //Rethrow caught Exception
                            throw;
                        }
                    }
                }

                //In the end of the consumption loop, check if scaleDown has been requested
                if (_scaleCallbackFunc())
                {
                    //If so, break consumption loop to let the thread end gracefully
                    break;
                }
            }

            //Loop ended, dispose ConsumerWorker
            Dispose();
        }

        private static string GetBody(BasicDeliverEventArgs basicDeliverEventArgs)
        {
            return Encoding.UTF8.GetString(basicDeliverEventArgs.Body);
        }

        public void Ack(ulong deliveryTag)
        {
            _model.BasicAck(deliveryTag, false);
        }

        public void Nack(ulong deliveryTag, bool requeue = false)
        {
            _model.BasicNack(deliveryTag, false, requeue);
        }

        public void Dispose()
        {
            _subscription.Close();
            _model.Dispose();
        }
    }
}
