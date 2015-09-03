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

        private readonly CancellationTokenSource _cancellationTokenSource;

        public AdvancedAsyncMessageProcessingWorker(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds)
        {
            _callbackFunc = callbackFunc;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public AdvancedAsyncMessageProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            ConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler)
        {
            _callbackFunc = callbackFunc;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public async static Task<AdvancedAsyncMessageProcessingWorker<T>> CreateAndStart(IQueueConsumer consumer,
            Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedAsyncMessageProcessingWorker<T>(consumer, callbackFunc, cancellationTokenSource,
            exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.Start();

            return instance;
        }

        public async static Task<AdvancedAsyncMessageProcessingWorker<T>> CreateAndStart(IQueueClient queueClient, 
            string queueName, Func<T, CancellationToken, Task> callbackFunc, CancellationTokenSource cancellationTokenSource, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedAsyncMessageProcessingWorker<T>(queueClient, queueName, callbackFunc, 
                cancellationTokenSource, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
                consumerCountManager, messageRejectionHandler);

            await instance.Start();

            return instance;
        }

        protected override async Task<bool> TryInvoke(T message, List<Exception> exceptions)
        {
            try
            {
                await _callbackFunc(message, _cancellationTokenSource.Token);

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
