using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumerWorker<T> : IQueueConsumerWorker
        where T : class
    {
        private bool _disposed;
        private readonly IModel _modelConsumer;
        private readonly IModel _modelPublisher;
        private readonly ILogger _logger;
        private readonly IQueueClient _queueClient;
        private readonly ISerializer _serializer;
        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;
        private readonly IMessageRejectionHandler _messageRejectionHandler;
        private readonly ushort _prefetchCount;
        private readonly string _queueName;

        public TimeSpan CheckAliveFrequency { get; set; }

        public RabbitMQConsumerWorker(ILogger logger, IQueueClient queueClient, IModel modelConsumer, IModel modelPublisher, string queueName,
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler,
            ISerializer serializer, ushort prefetchCount = 1)
        {
            _logger = logger;
            _queueClient = queueClient;
            _modelConsumer = modelConsumer;
            _modelPublisher = modelPublisher;
            _queueName = queueName;
            _prefetchCount = prefetchCount;
            _messageProcessingWorker = messageProcessingWorker;
            _messageRejectionHandler = messageRejectionHandler;
            _serializer = serializer;
            CheckAliveFrequency = new TimeSpan(0, 0, 10);
            SubscribeModelEvents();
        }

        private void SubscribeModelEvents()
        {
            _modelConsumer.CallbackException += _model_CallbackException;
            _modelConsumer.BasicRecoverOk += _model_BasicRecoverOk;
            _modelConsumer.ModelShutdown += _model_ModelShutdown;

            _modelPublisher.CallbackException += _model_CallbackException;
            _modelPublisher.BasicRecoverOk += _model_BasicRecoverOk;
            _modelPublisher.ModelShutdown += _model_ModelShutdown;
        }

        private void UnsubscribeModelEvents()
        {
            _modelConsumer.CallbackException -= _model_CallbackException;
            _modelConsumer.BasicRecoverOk -= _model_BasicRecoverOk;
            _modelConsumer.ModelShutdown -= _model_ModelShutdown;

            _modelPublisher.CallbackException -= _model_CallbackException;
            _modelPublisher.BasicRecoverOk -= _model_BasicRecoverOk;
            _modelPublisher.ModelShutdown -= _model_ModelShutdown;
        }

        private void _model_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQModel Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText} ");
        }

        private void _model_BasicRecoverOk(object sender, EventArgs e)
        {
            _logger?.LogInformation("RabbitMQModel Basic Recover Ok.");
        }

        private void _model_CallbackException(object sender, CallbackExceptionEventArgs e)
        {
            _logger?.LogError(e.Exception, $"RabbitMQModel Shutdown. Message: {e.Exception.Message}{Environment.NewLine}StackTrace: {e.Exception.StackTrace} ");
        }

        public async Task DoConsumeAsync(CancellationToken cancellationToken)
        {
            _modelConsumer.BasicQos(0, _prefetchCount, false);

            using (var subscription = new RabbitMQSubscription(_modelConsumer, _queueName, false))
            {
                //Iterate while thread hasn't been canceled
                while (!cancellationToken.IsCancellationRequested)
                {
                    //Create BasicDeliverEventArgs object and start trying to get the next message on the queue
                    var messageReceived = subscription.Next(Convert.ToInt32(CheckAliveFrequency.TotalMilliseconds),
                        out var lastResult);

                    if (messageReceived)
                    {
                        if (lastResult != null)
                        {
                            //Get message body
                            var messageBody = GetBody(lastResult);

                            try
                            {
                                //Try to deserialize message body into messageObject
                                var messageObject = _serializer.Deserialize<T>(messageBody);

                                //Create messageFeedbackSender instance with corresponding model and deliveryTag
                                IFeedbackSender feedbackSender = new RabbitMQMessageFeedbackSender(subscription, lastResult);

                                try
                                {
                                    //Call given messageProcessingWorker's OnMessage method to proceed with message processing
                                    var context = new RabbitMQConsumerContext(_queueClient, _modelPublisher);
                                    await _messageProcessingWorker.OnMessageAsync(messageObject, context, feedbackSender,
                                        cancellationToken).ConfigureAwait(false);

                                    //If message has been processed with no errors but no Acknoledgement has been given
                                    if (!feedbackSender.HasAcknoledged)
                                    {
                                        //Acknoledge message
                                        feedbackSender.Ack();
                                    }
                                }
                                catch (Exception e)
                                {
                                    _logger?.LogError(e, $"{e.Message}{Environment.NewLine}{e.StackTrace}");

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
                            catch (Exception exception)
                            {
                                _logger?.LogError(exception, $"{exception.Message}{Environment.NewLine}{exception.StackTrace}");

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
                                subscription.Nack(lastResult, false, false);
                            }
                        }
                    }
                    else
                    {
                        if (subscription.Model.IsClosed)
                        {
                            throw new Exception(
                                $"Model is already closed for RabbitMQConsumerWorker. Queue: {_queueName}");
                        }
                    }

                    subscription.Nack(true);
                }
            }
        }

        private static string GetBody(BasicDeliverEventArgs basicDeliverEventArgs)
        {
            return Encoding.UTF8.GetString(basicDeliverEventArgs.Body);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            UnsubscribeModelEvents();
            _modelPublisher.Close();
            _modelConsumer.Close();

            _disposed = true;
        }
    }
}
