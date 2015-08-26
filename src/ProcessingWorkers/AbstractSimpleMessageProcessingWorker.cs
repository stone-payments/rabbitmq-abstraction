using System;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public abstract class AbstractSimpleMessageProcessingWorker<T> : IMessageProcessingWorker<T>, IDisposable where T : class
    {
        protected IQueueConsumer Consumer;

        protected IQueueClient QueueClient;

        protected string QueueName;

        protected IConsumerCountManager ConsumerCountManager;

        protected IMessageRejectionHandler MessageRejectionHandler;

        protected AbstractSimpleMessageProcessingWorker(IQueueConsumer consumer, bool autoStartup = true)
        {
            Consumer = consumer;

            if (autoStartup)
            {
                Start();
            }
        }

        protected AbstractSimpleMessageProcessingWorker(IQueueClient queueClient, string queueName,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            bool autoStartup = true)
        {
            QueueClient = queueClient;
            QueueName = queueName;

            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler = messageRejectionHandler ?? new MessageDeserializationRejectionHandler(QueueClient);

            if (autoStartup)
            {
                Start();
            }
        }

        public void Start()
        {
            if (Consumer == null)
            {
                Consumer = QueueClient.GetConsumer<T>(QueueName, ConsumerCountManager, this,
                    MessageRejectionHandler);
            }

            Consumer.Start();
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

        public abstract void OnMessage(T message, IMessageFeedbackSender feedbackSender);
    }
}
