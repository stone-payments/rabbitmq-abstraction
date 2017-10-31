using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class AdvancedAsyncProcessingWorker<T> : AbstractAdvancedProcessingWorker<T> where T : class
    {
        private readonly Func<T, CancellationToken, Task> _callbackFunc;

        private readonly TimeSpan _processingTimeout;

        private readonly Func<IEnumerable<T>, CancellationToken, Task> _batchCallbackFunc;

        private readonly ushort _batchSize;

        public AdvancedAsyncProcessingWorker(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public AdvancedAsyncProcessingWorker(IQueueConsumer consumer,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public AdvancedAsyncProcessingWorker(IQueueClient queueClient, string queueName, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public AdvancedAsyncProcessingWorker(IQueueClient queueClient, string queueName,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, CancellationToken cancellationToken, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(consumer, callbackFunc, processingTimeout, 
            exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout,
            CancellationToken cancellationToken, ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(consumer, batchCallbackFunc, batchSize, processingTimeout,
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, CancellationToken cancellationToken, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(queueClient, queueName, callbackFunc, 
                processingTimeout, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout,
            CancellationToken cancellationToken, ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(queueClient, queueName, batchCallbackFunc, batchSize, processingTimeout, 
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task<Task> StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackFunc != null);
        }

        protected override async Task<bool> TryInvokeAsync(T message, List<Exception> exceptions, 
            CancellationToken cancellationToken)
        {
            try
            {
                using (var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    tokenSource.CancelAfter(_processingTimeout);

                    await _callbackFunc(message, tokenSource.Token).ConfigureAwait(false);
                }

                return true;
            }
            catch (Exception exception)
            {
                exceptions.Add(exception);

                return false;
            }
        }

        protected override async Task<bool> TryInvokeBatchAsync(IEnumerable<T> batch, List<Exception> exceptions, CancellationToken cancellationToken)
        {
            if(_batchCallbackFunc == null)
            {
                throw new Exception("Undefined batch callback function");
            }

            try
            {
                using (var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    tokenSource.CancelAfter(_processingTimeout);

                    await _batchCallbackFunc(batch, tokenSource.Token).ConfigureAwait(false);
                }

                return true;
            }
            catch (Exception exception)
            {
                exceptions.Add(exception);

                return false;
            }
        }

        public override ushort GetBatchSize()
        {
            return _batchSize;
        }
    }
}
