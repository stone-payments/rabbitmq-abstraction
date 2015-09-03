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

        private readonly CancellationTokenSource _cancellationTokenSource;

        public SimpleAsyncMessageProcessingWorker(IQueueConsumer consumer, Func<T, CancellationToken, Task> callbackFunc, 
            CancellationTokenSource cancellationTokenSource)
            : base(consumer)
        {
            _callbackFunc = callbackFunc;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public SimpleAsyncMessageProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            _callbackFunc = callbackFunc;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public async static Task<SimpleAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource)
        {
            var instance = new SimpleAsyncMessageProcessingWorker<T>(consumer, callbackFunc, cancellationTokenSource);

            await instance.StartAsync();

            return instance;
        }

        public async static Task<SimpleAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Func<T, CancellationToken, Task> callbackFunc, 
            CancellationTokenSource cancellationTokenSource, IConsumerCountManager consumerCountManager = null, 
            IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleAsyncMessageProcessingWorker<T>(queueClient, queueName, callbackFunc, 
                cancellationTokenSource, consumerCountManager, messageRejectionHandler);

            await instance.StartAsync();

            return instance;
        }

        public async override Task OnMessageAsync(T message, IMessageFeedbackSender feedbackSender)
        {
            try
            {
                await _callbackFunc(message, _cancellationTokenSource.Token);
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }
    }
}
