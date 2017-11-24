using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class AdvancedProcessingWorker<T> : AbstractAdvancedProcessingWorker<T> where T : class
    {
        private readonly Action<T> _callbackAction;

        private readonly Action<IEnumerable<T>> _batchCallbackAction;

        private readonly ushort _batchSize;

        public AdvancedProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger)
        {
            _callbackAction = callbackAction;
        }

        public AdvancedProcessingWorker(IQueueConsumer consumer, Action<IEnumerable<T>> batchCallbackAction,
            ushort batchSize, ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, logger)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public AdvancedProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler, logger)
        {
            _callbackAction = callbackAction;
        }

        public AdvancedProcessingWorker(IQueueClient queueClient, string queueName, Action<IEnumerable<T>> batchCallbackAction,
            ushort batchSize, ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds,
                consumerCountManager, messageRejectionHandler, logger)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public static async Task<AdvancedProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Action<T> callbackAction, CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
        {
            var instance = new AdvancedProcessingWorker<T>(consumer, callbackAction, exceptionHandlingStrategy, 
                invokeRetryCount, invokeRetryWaitMilliseconds, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, CancellationToken cancellationToken, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
        {
            var instance = new AdvancedProcessingWorker<T>(consumer, batchCallbackAction, batchSize, exceptionHandlingStrategy,
                invokeRetryCount, invokeRetryWaitMilliseconds, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, Action<T> callbackAction, CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null)
        {
            var instance = new AdvancedProcessingWorker<T>(queueClient, queueName, callbackAction,
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, consumerCountManager, 
                messageRejectionHandler, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, CancellationToken cancellationToken, Action<IEnumerable<T>> batchCallbackAction, ushort batchSize,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null)
        {
            var instance = new AdvancedProcessingWorker<T>(queueClient, queueName, batchCallbackAction, batchSize,
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, consumerCountManager,
                messageRejectionHandler, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task<Task> StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackAction != null);
        }

        protected override Task<bool> TryInvokeAsync(T message, List<Exception> exceptions, 
            CancellationToken cancellationToken)
        {
            try
            {
                _callbackAction(message);

                return Task.FromResult(true);
            }
            catch (Exception exception)
            {
                exceptions.Add(exception);

                return Task.FromResult(false);
            }
        }

        protected override Task<bool> TryInvokeBatchAsync(IEnumerable<T> batch, List<Exception> exceptions, CancellationToken cancellationToken)
        {
            if (_batchCallbackAction == null)
            {
                throw new Exception("Undefined batch callback action");
            }

            try
            {
                _batchCallbackAction(batch);

                return Task.FromResult(true);
            }
            catch (Exception exception)
            {
                exceptions.Add(exception);

                return Task.FromResult(false);
            }
        }

        public override ushort GetBatchSize()
        {
            return _batchSize;
        }
    }
}
