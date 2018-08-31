using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQBatchConsumerWorker<T> : IQueueConsumerWorker where T : class
    {
        private readonly string _queueName;
        private readonly ILogger _logger;
        private readonly IModel _model;
        private readonly ISerializer _serializer;
        private readonly IBatchProcessingWorker<T> _batchProcessingWorker;
        private readonly IMessageRejectionHandler _messageRejectionHandler;

        public TimeSpan CheckAliveFrequency { get; set; }

        public RabbitMQBatchConsumerWorker(ILogger logger, RabbitMQConnection connection, string queueName, IModel model,
            IBatchProcessingWorker<T> batchProcessingWorker, IMessageRejectionHandler messageRejectionHandler,
            ISerializer serializer)
        {
            _logger = logger;
            _model = model;
            _model.BasicQos(0, batchProcessingWorker.GetBatchSize(), false);
            _queueName = queueName;
            _batchProcessingWorker = batchProcessingWorker;
            _messageRejectionHandler = messageRejectionHandler;
            _serializer = serializer;
            CheckAliveFrequency = new TimeSpan(0, 0, 10);
        }

        public async Task DoConsumeAsync(CancellationToken cancellationToken)
        {
            //Iterate while thread hasn't been canceled
            while (!cancellationToken.IsCancellationRequested)
            {
                var currentBatchCounter = _batchProcessingWorker.GetBatchSize();

                var currentBatch = new List<T>();

                BasicGetResult result;

                ulong lastDeliveryTag = 0;

                do
                {
                    result = _model.BasicGet(_queueName, false);

                    if (result != null)
                    {
                        lastDeliveryTag = result.DeliveryTag;

                        currentBatchCounter--;

                        var messageBody = GetBody(result);

                        try
                        {
                            var messageObject = _serializer.Deserialize<T>(messageBody);

                            currentBatch.Add(messageObject);
                        }
                        catch (Exception exception)
                        {
                            var deserializationException = new DeserializationException("Unable to deserialize data.", exception)
                            {
                                SerializedDataString = messageBody,
                                SerializedDataBinary = result.Body,
                                QueueName = _queueName
                            };

                            await _messageRejectionHandler.OnRejectionAsync(deserializationException).ConfigureAwait(false);

                            //Remove message from queue after RejectionHandler dealt with it
                            _model.BasicNack(result.DeliveryTag, false, false);
                        }
                    }
                } while (currentBatchCounter > 0 && result != null);

                if (currentBatch.Count > 0)
                {
                    var batchFeedbackSender = new RabbitMQBatchFeedbackSender(_model, lastDeliveryTag);

                    try
                    {
                        await _batchProcessingWorker.OnBatchAsync(currentBatch, batchFeedbackSender,
                            cancellationToken).ConfigureAwait(false);

                        if (!batchFeedbackSender.HasAcknoledged)
                        {
                            //Acknoledge message
                            batchFeedbackSender.Ack();
                        }
                    }
                    catch (Exception e)
                    {
                        //If something went wrong with message processing and message hasn't been acknoledged yet
                        if (!batchFeedbackSender.HasAcknoledged)
                        {
                            //Negatively Acknoledge message, asking for requeue
                            batchFeedbackSender.Nack(true);
                        }

                        //Rethrow caught Exception
                        throw;
                    }
                }
            }

            //Loop ended, dispose ConsumerWorker
            Dispose();
        }

        private static string GetBody(BasicGetResult basicGetResult)
        {
            return Encoding.UTF8.GetString(basicGetResult.Body);
        }

        public void Dispose()
        {
            _model.Dispose();
        }
    }
}
