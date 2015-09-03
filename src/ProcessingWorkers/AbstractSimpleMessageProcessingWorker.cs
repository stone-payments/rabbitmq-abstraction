﻿using System;
using System.Threading.Tasks;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public abstract class AbstractSimpleMessageProcessingWorker<T> : IMessageProcessingWorker<T>, IDisposable where T : class
    {
        protected IQueueConsumer Consumer;

        protected readonly IQueueClient QueueClient;

        protected readonly string QueueName;

        protected readonly IConsumerCountManager ConsumerCountManager;

        protected readonly IMessageRejectionHandler MessageRejectionHandler;

        protected AbstractSimpleMessageProcessingWorker(IQueueConsumer consumer)
        {
            Consumer = consumer;
        }

        protected AbstractSimpleMessageProcessingWorker(IQueueClient queueClient, string queueName,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            QueueClient = queueClient;
            QueueName = queueName;

            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler = messageRejectionHandler ?? new MessageDeserializationRejectionHandler(QueueClient);
        }

        public async Task Start()
        {
            if (Consumer == null)
            {
                Consumer = QueueClient.GetConsumer(QueueName, ConsumerCountManager, this,
                    MessageRejectionHandler);
            }

            await Consumer.Start();
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

        public abstract Task OnMessage(T message, IMessageFeedbackSender feedbackSender);
    }
}
