using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public class AdvancedMessageProcessingWorker<T> : AbstractAdvancedMessageProcessingWorker<T> where T : class
    {
        private readonly Action<T> _callbackAction;

        public AdvancedMessageProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds)
        {
            _callbackAction = callbackAction;
        }

        public AdvancedMessageProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            ConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler)
        {
            _callbackAction = callbackAction;
        }

        public static async Task<AdvancedMessageProcessingWorker<T>> CreateAndStartAsync(IQueueConsumer consumer, 
            Action<T> callbackAction, CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedMessageProcessingWorker<T>(consumer, callbackAction, exceptionHandlingStrategy, 
                invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
        }

        public static async Task<AdvancedMessageProcessingWorker<T>> CreateAndStartAsync(IQueueClient queueClient,
            string queueName, Action<T> callbackAction, CancellationToken cancellationToken,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, 
            ConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedMessageProcessingWorker<T>(queueClient, queueName, callbackAction, 
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, consumerCountManager, 
                messageRejectionHandler);

            await instance.StartAsync(cancellationToken).ConfigureAwait(false);

            return instance;
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
    }
}
