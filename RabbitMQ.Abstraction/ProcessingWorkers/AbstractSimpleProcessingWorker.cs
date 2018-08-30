using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public abstract class AbstractSimpleProcessingWorker<T> : IMessageProcessingWorker<T>, IBatchProcessingWorker<T>, IDisposable where T : class
    {
        protected IQueueConsumer Consumer;

        protected readonly IQueueClient QueueClient;

        protected readonly string QueueName;

        protected readonly IConsumerCountManager ConsumerCountManager;

        protected readonly IMessageRejectionHandler MessageRejectionHandler;

        protected readonly ILogger Logger;

        protected readonly ushort PrefetchCount;

        protected AbstractSimpleProcessingWorker(IQueueConsumer consumer, ILogger logger = null)
        {
            Consumer = consumer;
            Logger = logger;
        }

        protected AbstractSimpleProcessingWorker(IQueueClient queueClient, string queueName,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            QueueClient = queueClient;
            QueueName = queueName;

            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler =
                messageRejectionHandler ?? new MessageDeserializationRejectionHandler(QueueClient);

            Logger = logger;

            PrefetchCount = prefetchCount;
        }

        protected Task StartAsync(CancellationToken cancellationToken, bool batched)
        {
            if (Consumer == null)
            {
                if (batched)
                {
                    Consumer = QueueClient.CreateBatchConsumer(QueueName, ConsumerCountManager, this,
                        MessageRejectionHandler);
                }
                else
                {
                    Consumer = QueueClient.CreateConsumer(QueueName, ConsumerCountManager, this,
                        MessageRejectionHandler, PrefetchCount);
                }
            }

            return Consumer.StartAsync(cancellationToken);
        }

        public void Stop()
        {
            if (Consumer != null)
            {
                Consumer.Stop();
                Consumer = null;
            }
        }

        public void Dispose()
        {
            Stop();
        }

        public abstract Task OnMessageAsync(T message, RabbitMQConsumerContext consumerContext, IFeedbackSender feedbackSender, CancellationToken cancellationToken);

        public abstract Task OnBatchAsync(IEnumerable<T> batch, IFeedbackSender feedbackSender, CancellationToken cancellationToken);

        public abstract ushort GetBatchSize();
    }
}
