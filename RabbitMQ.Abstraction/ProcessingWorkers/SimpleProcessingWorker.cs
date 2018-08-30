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
    public class SimpleProcessingWorker<T> : AbstractSimpleProcessingWorker<T> where T : class
    {
        private readonly Action<T, RabbitMQConsumerContext> _callbackAction;

        private readonly Action<IEnumerable<T>> _batchCallbackAction;

        private readonly ushort _batchSize;

        public SimpleProcessingWorker(IQueueConsumer consumer, Action<T, RabbitMQConsumerContext> callbackAction, ILogger logger = null)
            : base(consumer, logger)
        {
            _callbackAction = callbackAction;
        }

        public SimpleProcessingWorker(IQueueConsumer consumer, Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, ILogger logger = null)
            : base(consumer, logger)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public SimpleProcessingWorker(IQueueClient queueClient, string queueName, Action<T, RabbitMQConsumerContext> callbackAction,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _callbackAction = callbackAction;
        }

        public SimpleProcessingWorker(IQueueClient queueClient, string queueName,
            Action<IEnumerable<T>> batchCallbackAction,
            ushort batchSize, IConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Action<T, RabbitMQConsumerContext> callbackAction, CancellationToken cancellationToken, ILogger logger = null)
        {
            var instance = new SimpleProcessingWorker<T>(consumer, callbackAction, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, CancellationToken cancellationToken, ILogger logger = null)
        {
            var instance = new SimpleProcessingWorker<T>(consumer, batchCallbackAction, batchSize, logger);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName,
            Action<T, RabbitMQConsumerContext> callbackAction, CancellationToken cancellationToken,
            IConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new SimpleProcessingWorker<T>(queueClient, queueName, callbackAction,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName,
            Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, CancellationToken cancellationToken,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ILogger logger = null, ushort prefetchCount = 1)
        {
            var instance = new SimpleProcessingWorker<T>(queueClient, queueName, batchCallbackAction, batchSize,
                consumerCountManager, messageRejectionHandler, logger, prefetchCount);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackAction != null);
        }

        public override Task OnMessageAsync(T message, RabbitMQConsumerContext consumerContext, IFeedbackSender feedbackSender,
            CancellationToken cancellationToken)
        {
            try
            {
                _callbackAction(message, consumerContext);
                feedbackSender.Ack();
            }
            catch (Exception e)
            {
                feedbackSender.Nack(true);
            }

            return Task.FromResult(0);
        }

        public override Task OnBatchAsync(IEnumerable<T> batch, IFeedbackSender feedbackSender, CancellationToken cancellationToken)
        {
            if (_batchCallbackAction == null)
            {
                throw new Exception("Undefined batch callback action");
            }

            try
            {
                _batchCallbackAction(batch);
                feedbackSender.Ack();
            }
            catch (Exception e)
            {
                feedbackSender.Nack(true);
            }

            return Task.FromResult(0);
        }

        public override ushort GetBatchSize()
        {
            return _batchSize;
        }
    }
}
