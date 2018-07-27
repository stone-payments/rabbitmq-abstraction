using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumerWorker<T> : IQueueConsumerWorker where T : class
    {
        private readonly IRabbitMQConnection _connection;
        private readonly ISerializer _serializer;
        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;
        private readonly IMessageRejectionHandler _messageRejectionHandler;
        private readonly Func<bool> _scaleCallbackFunc;
        private readonly ushort _prefetchCount;
        private readonly string _queueName;

        public TimeSpan CheckAliveFrequency { get; set; }

        public RabbitMQConsumerWorker(IRabbitMQConnection connection, string queueName, 
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler, 
            ISerializer serializer, Func<bool> scaleCallbackFunc, ushort prefetchCount = 1)
        {
            _queueName = queueName;
            _prefetchCount = prefetchCount;
            _connection = connection;
            _messageProcessingWorker = messageProcessingWorker;
            _messageRejectionHandler = messageRejectionHandler;
            _serializer = serializer;
            _scaleCallbackFunc = scaleCallbackFunc;
            CheckAliveFrequency = new TimeSpan(0, 0, 10);
        }

        public async Task DoConsumeAsync(CancellationToken cancellationToken)
        {
            using (var model = await _connection.GetModelAsync())
            {
                model.BasicQos(0, _prefetchCount, false);

                using (var subscription = new RabbitMQSubscription(model, _queueName, false))
                {
                    //Iterate while thread hasn't been canceled
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        //Create BasicDeliverEventArgs object and start trying to get the next message on the queue
                        var messageReceived = subscription.Next(Convert.ToInt32(CheckAliveFrequency.TotalMilliseconds),
                            out BasicDeliverEventArgs lastResult);

                        //If a message hasn't been succesfully fetched from the queue
                        if (!messageReceived)
                        {
                            //If the model has been closed
                            if (!model.IsOpen)
                            {
                                //Throw AlreadyClosedException (model is already closed)
                                throw new global::RabbitMQ.Client.Exceptions.AlreadyClosedException(model.CloseReason);
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
                                    QueueName = subscription.QueueName
                                };
                                //Pass DeserializationException to RejectionHandler
                                await _messageRejectionHandler.OnRejectionAsync(deserializationException).ConfigureAwait(false);

                                //Remove message from queue after RejectionHandler dealt with it
                                model.BasicNack(lastResult.DeliveryTag, false, false);
                            }

                            //If message has been successfully deserialized and messageObject is populated
                            if (messageObject != null)
                            {
                                //Create messageFeedbackSender instance with corresponding model and deliveryTag
                                IFeedbackSender feedbackSender = new RabbitMQMessageFeedbackSender(model, lastResult.DeliveryTag);

                                try
                                {
                                    //Call given messageProcessingWorker's OnMessage method to proceed with message processing
                                    await _messageProcessingWorker.OnMessageAsync(messageObject, feedbackSender,
                                        cancellationToken).ConfigureAwait(false);

                                    //If message has been processed with no errors but no Acknoledgement has been given
                                    if (!feedbackSender.HasAcknoledged)
                                    {
                                        //Acknoledge message
                                        feedbackSender.Ack();
                                    }
                                }
                                catch (Exception)
                                {
                                    //If something went wrong with message processing and message hasn't been acknoledged yet
                                    if (!feedbackSender.HasAcknoledged)
                                    {
                                        //Negatively Acknoledge message, asking for requeue
                                        feedbackSender.Nack(true);
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
                }
            }
        }

        private static string GetBody(BasicDeliverEventArgs basicDeliverEventArgs)
        {
            return Encoding.UTF8.GetString(basicDeliverEventArgs.Body);
        }
    }
}
