using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;
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

        protected AbstractSimpleProcessingWorker(IQueueConsumer consumer)
        {
            Consumer = consumer;
        }

        protected AbstractSimpleProcessingWorker(IQueueClient queueClient, string queueName,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            QueueClient = queueClient;
            QueueName = queueName;

            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler = messageRejectionHandler ?? new MessageDeserializationRejectionHandler(QueueClient);
        }

        public Task StartAsync(CancellationToken cancellationToken, bool batched = false)
        {
            if (Consumer == null)
            {
                if (batched)
                {
                    Consumer = QueueClient.GetBatchConsumer(QueueName, ConsumerCountManager, this,
                        MessageRejectionHandler);
                }
                else
                {
                    Consumer = QueueClient.GetConsumer(QueueName, ConsumerCountManager, this,
                        MessageRejectionHandler);
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

        public abstract Task OnMessageAsync(T message, IFeedbackSender feedbackSender, CancellationToken cancellationToken);

        public abstract Task OnBatchAsync(IEnumerable<T> batch, IFeedbackSender feedbackSender, CancellationToken cancellationToken);

        public abstract ushort GetBatchSize();
    }
}
