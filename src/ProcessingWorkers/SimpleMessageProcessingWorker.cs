using System;
using System.Threading;
using System.Threading.Tasks;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public class SimpleMessageProcessingWorker<T> : AbstractSimpleMessageProcessingWorker<T> where T : class
    {
        protected readonly Action<T> CallbackAction;

        public SimpleMessageProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction)
            : base(consumer)
        {
            CallbackAction = callbackAction;
        }

        public SimpleMessageProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            CallbackAction = callbackAction;
        }

        public async static Task<SimpleMessageProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Action<T> callbackAction, CancellationToken cancellationToken)
        {
            var instance = new SimpleMessageProcessingWorker<T>(consumer, callbackAction);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public async static Task<SimpleMessageProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Action<T> callbackAction, CancellationToken cancellationToken,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleMessageProcessingWorker<T>(queueClient, queueName, callbackAction, 
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public override Task OnMessageAsync(T message, IMessageFeedbackSender feedbackSender, 
            CancellationToken cancellationToken)
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

            return Task.FromResult(0);
        }
    }
}
