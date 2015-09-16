using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public class AdvancedAsyncMessageProcessingWorker<T> : AbstractAdvancedMessageProcessingWorker<T> where T : class
    {
        private readonly Func<T, CancellationToken, Task> _callbackFunc;

        private readonly TimeSpan _processingTimeout;

        public AdvancedAsyncMessageProcessingWorker(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public AdvancedAsyncMessageProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            ConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public async static Task<AdvancedAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, 
            CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedAsyncMessageProcessingWorker<T>(consumer, callbackFunc, processingTimeout,
            exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public async static Task<AdvancedAsyncMessageProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, 
            CancellationToken cancellationToken, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            ConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedAsyncMessageProcessingWorker<T>(queueClient, queueName, callbackFunc, 
                processingTimeout, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        protected override async Task<bool> TryInvokeAsync(T message, List<Exception> exceptions, 
            CancellationToken cancellationToken)
        {
            try
            {
                var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                tokenSource.CancelAfter(_processingTimeout);

                await _callbackFunc(message, tokenSource.Token).ConfigureAwait(false);

                return true;
            }
            catch (Exception exception)
            {
                exceptions.Add(exception);

                return false;
            }
        }
    }
}
