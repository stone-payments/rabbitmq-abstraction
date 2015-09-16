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

        private readonly TimeSpan _processingTimeout;

        public SimpleAsyncMessageProcessingWorker(IQueueConsumer consumer, Func<T, CancellationToken, Task> callbackFunc, 
            TimeSpan processingTimeout)
            : base(consumer)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public SimpleAsyncMessageProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public async static Task<SimpleAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, CancellationToken cancellationToken)
        {
            var instance = new SimpleAsyncMessageProcessingWorker<T>(consumer, callbackFunc, processingTimeout);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public async static Task<SimpleAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, 
            CancellationToken cancellationToken, IConsumerCountManager consumerCountManager = null, 
            IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleAsyncMessageProcessingWorker<T>(queueClient, queueName, callbackFunc,
                processingTimeout, consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public async override Task OnMessageAsync(T message, IMessageFeedbackSender feedbackSender, CancellationToken cancellationToken)
        {
            try
            {
                var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                tokenSource.CancelAfter(_processingTimeout);

                await _callbackFunc(message, tokenSource.Token).ConfigureAwait(false);
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }
    }
}
