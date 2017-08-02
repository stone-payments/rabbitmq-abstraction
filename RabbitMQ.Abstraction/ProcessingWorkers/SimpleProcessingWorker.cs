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

        public SimpleProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction, 
            Action<IEnumerable<T>> batchCallbackAction = null, ushort batchSize = 1)
            : base(consumer)
        {
            _callbackAction = callbackAction;
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public SimpleProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction, 
            Action<IEnumerable<T>> batchCallbackAction = null, ushort batchSize = 1,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler)
        {
            _callbackAction = callbackAction;
            _batchCallbackAction = batchCallbackAction;
            _batchSize = batchSize;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Action<T> callbackAction, CancellationToken cancellationToken, Action<IEnumerable<T>> batchCallbackAction = null, 
            ushort batchSize = 1)
        {
            var instance = new SimpleProcessingWorker<T>(consumer, callbackAction, batchCallbackAction, batchSize);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<SimpleProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient, 
            string queueName, Action<T> callbackAction, CancellationToken cancellationToken, Action<IEnumerable<T>> batchCallbackAction = null, 
            ushort batchSize = 1, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new SimpleProcessingWorker<T>(queueClient, queueName, callbackAction, batchCallbackAction, batchSize,
                consumerCountManager, messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
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
