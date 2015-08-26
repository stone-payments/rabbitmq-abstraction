using System;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public class SimpleMessageProcessingWorker<T> : AbstractSimpleMessageProcessingWorker<T> where T : class
    {
        protected readonly Action<T> CallbackAction;

        public SimpleMessageProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction, bool autoStartup = true) 
            : base(consumer, autoStartup)
        {
            CallbackAction = callbackAction;
        }

        public SimpleMessageProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, 
            bool autoStartup = true) 
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, autoStartup)
        {
            CallbackAction = callbackAction;
        }

        public override void OnMessage(T message, IMessageFeedbackSender feedbackSender)
        {
            try
            {
                CallbackAction(message);
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }
    }
}
