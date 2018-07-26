using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class SimpleAsyncProcessingWorker<T> : AbstractSimpleProcessingWorker<T> where T : class
    {
        private readonly Func<T, CancellationToken, Task> _callbackFunc;

        private readonly TimeSpan _processingTimeout;

        private readonly Func<IEnumerable<T>, CancellationToken, Task> _batchCallbackFunc;

        private readonly ushort _batchSize;

        public SimpleAsyncProcessingWorker(IQueueConsumer consumer, Func<T, CancellationToken, Task> callbackFunc, 
            TimeSpan processingTimeout, ILogger logger = null)
            : base(consumer, logger)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public SimpleAsyncProcessingWorker(IQueueConsumer consumer, Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, 
            ushort batchSize, TimeSpan processingTimeout, ILogger logger = null)
            : base(consumer, logger)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public SimpleAsyncProcessingWorker(IQueueClient queueClient, string queueName,
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _callbackFunc = callbackFunc;
            _processingTimeout = processingTimeout;
        }

        public SimpleAsyncProcessingWorker(IQueueClient queueClient, string queueName,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize,
            TimeSpan processingTimeout,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _batchCallbackFunc = batchCallbackFunc;
            _processingTimeout = processingTimeout;
            _batchSize = batchSize;
        }

        public static async Task<SimpleAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout, CancellationToken cancellationToken, ILogger logger = null)
        {
            var instance = new SimpleAsyncProcessingWorker<T>(consumer, callbackFunc, processingTimeout, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize, TimeSpan processingTimeout, 
            CancellationToken cancellationToken, ILogger logger = null)
        {
            var instance = new SimpleAsyncProcessingWorker<T>(consumer, batchCallbackFunc, batchSize, processingTimeout, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName,
            Func<T, CancellationToken, Task> callbackFunc, TimeSpan processingTimeout,
            CancellationToken cancellationToken,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new SimpleAsyncProcessingWorker<T>(queueClient, queueName, callbackFunc,
                processingTimeout, consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleAsyncProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName,
            Func<IEnumerable<T>, CancellationToken, Task> batchCallbackFunc, ushort batchSize,
            TimeSpan processingTimeout, CancellationToken cancellationToken,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new SimpleAsyncProcessingWorker<T>(queueClient, queueName, batchCallbackFunc, batchSize,
                processingTimeout,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task<Task> StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackFunc != null);
        }

        public override async Task OnMessageAsync(T message, IFeedbackSender feedbackSender, CancellationToken cancellationToken)
        {
            try
            {
                using (var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    tokenSource.CancelAfter(_processingTimeout);

                    await _callbackFunc(message, tokenSource.Token).ConfigureAwait(false);
                }
                
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }

        public override async Task OnBatchAsync(IEnumerable<T> batch, IFeedbackSender feedbackSender, CancellationToken cancellationToken)
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
                }
                
                feedbackSender.Ack();
            }
            catch (Exception)
            {
                feedbackSender.Nack(true);
            }
        }

        public override ushort GetBatchSize()
        {
            return _batchSize;
        }
    }
}
