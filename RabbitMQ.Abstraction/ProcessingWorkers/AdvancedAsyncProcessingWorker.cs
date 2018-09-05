using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class AdvancedAsyncProcessingWorker<T> : AbstractAdvancedProcessingWorker<T> where T : class
    {
        private readonly Func<T, RabbitMQConsumerContext, CancellationToken, Task> _callbackFunc;

        private readonly TimeSpan _processingTimeout;

        private readonly Func<IEnumerable<T>, CancellationToken, Task> _batchCallbackFunc;

        private readonly ushort _batchSize;

        private readonly ILogger _logger;

        public AdvancedAsyncProcessingWorker(IQueueConsumer consumer,
            Func<T, RabbitMQConsumerContext, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;

            _logger = logger;
        }

        public AdvancedAsyncProcessingWorker(IQueueConsumer consumer,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public AdvancedAsyncProcessingWorker(IQueueClient queueClient, string queueName,
            Func<T, RabbitMQConsumerContext, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public AdvancedAsyncProcessingWorker(IQueueClient queueClient, string queueName,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize,
            TimeSpan processingTimeout,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<T, RabbitMQConsumerContext, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(consumer, callbackFunc, processingTimeout,
            exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout,
            CancellationToken cancellationToken, ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(consumer, batchCallbackFunc, batchSize, processingTimeout,
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, Func<T, RabbitMQConsumerContext, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(queueClient, queueName, callbackFunc,
                processingTimeout, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize,
            TimeSpan processingTimeout,
            CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new AdvancedAsyncProcessingWorker<T>(queueClient, queueName, batchCallbackFunc, batchSize,
                processingTimeout,
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackFunc != null);
        }

        protected override async Task<bool> TryInvokeAsync(T message, RabbitMQConsumerContext consumerContext, List<Exception> exceptions,
            CancellationToken cancellationToken)
        {
            try
            {
                using (var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    tokenSource.CancelAfter(_processingTimeout);

                    await _callbackFunc(message, consumerContext, tokenSource.Token).ConfigureAwait(false);

                    if (tokenSource.IsCancellationRequested)
                    {
                        _logger?.LogError($"Task cancelled after timeout: {_processingTimeout.Milliseconds} ms");
                    }
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
            if (_batchCallbackFunc == null)
            {
                throw new Exception("Undefined batch callback function");
            }

            try
            {
                using (var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    tokenSource.CancelAfter(_processingTimeout);

                    await _batchCallbackFunc(batch, tokenSource.Token).ConfigureAwait(false);

                    if (tokenSource.IsCancellationRequested)
                    {
                        _logger?.LogError($"Task cancelled after timeout: {_processingTimeout.Milliseconds} ms");
                    }
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
