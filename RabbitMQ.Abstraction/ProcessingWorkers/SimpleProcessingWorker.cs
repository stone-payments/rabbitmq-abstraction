using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class SimpleProcessingWorker<T> : AbstractSimpleProcessingWorker<T> where T : class
    {
        private readonly Action<T> _callbackAction;

        private readonly Action<IEnumerable<T>> _batchCallbackAction;

        private readonly ushort _batchSize;

        public SimpleProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction)
            : base(consumer)
        {
            _callbackAction = callbackAction;
        }

        public SimpleProcessingWorker(IQueueConsumer consumer, Action<IEnumerable<T>> batchCallbackAction, ushort batchSize)
            : base(consumer)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public SimpleProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            _callbackAction = callbackAction;
        }

        public SimpleProcessingWorker(IQueueClient queueClient, string queueName, Action<IEnumerable<T>> batchCallbackAction, 
            ushort batchSize, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Action<T> callbackAction, CancellationToken cancellationToken)
        {
            var instance = new SimpleProcessingWorker<T>(consumer, callbackAction);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer,
            Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, CancellationToken cancellationToken)
        {
            var instance = new SimpleProcessingWorker<T>(consumer, batchCallbackAction, batchSize);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, string queueName, 
            Action<T> callbackAction, CancellationToken cancellationToken, IConsumerCountManager consumerCountManager = null, 
            IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleProcessingWorker<T>(queueClient, queueName, callbackAction,
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, string queueName, 
            Action<IEnumerable<T>> batchCallbackAction, ushort batchSize, CancellationToken cancellationToken, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleProcessingWorker<T>(queueClient, queueName, batchCallbackAction, batchSize,
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return StartAsync(cancellationToken, _batchCallbackAction != null);
        }

        public override Task OnMessageAsync(T message, IFeedbackSender feedbackSender, 
            CancellationToken cancellationToken)
        {
            try
            {
                _callbackAction(message);
                feedbackSender.Ack();
            }
            catch (Exception)
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
            catch (Exception)
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
