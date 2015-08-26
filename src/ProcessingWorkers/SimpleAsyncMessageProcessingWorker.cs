using System;
using System.Threading;
using System.Threading.Tasks;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public class SimpleAsyncMessageProcessingWorker<T> : AbstractSimpleMessageProcessingWorker<T> where T : class
    {
        private readonly Func<T, CancellationToken, Task> _callbackFunc;

        private readonly CancellationToken _cancellationToken;

        public SimpleAsyncMessageProcessingWorker(IQueueConsumer consumer, Func<T, CancellationToken, Task> callbackFunc, 
            CancellationToken cancellationToken, bool autoStartup = true) 
            : base(consumer, autoStartup)
        {
            _callbackFunc = callbackFunc;
            _cancellationToken = cancellationToken;
        }

        public SimpleAsyncMessageProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, CancellationToken cancellationToken, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, 
            bool autoStartup = true) 
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, autoStartup)
        {
            _callbackFunc = callbackFunc;
            _cancellationToken = cancellationToken;
        }

        public async override void OnMessage(T message, IMessageFeedbackSender feedbackSender)
        {
            try
            {
                await _callbackFunc(message, _cancellationToken);
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }
    }
}
