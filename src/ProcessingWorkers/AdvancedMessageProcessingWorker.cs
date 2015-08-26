using System;
using System.Collections.Generic;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
{
    public class AdvancedMessageProcessingWorker<T> : AbstractAdvancedMessageProcessingWorker<T> where T : class
    {
        private readonly Action<T> _callbackAction;

        public AdvancedMessageProcessingWorker(IQueueConsumer consumer, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, bool autoStartup = true)
            : base(consumer, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, autoStartup)
        {
            _callbackAction = callbackAction;
        }

        public AdvancedMessageProcessingWorker(IQueueClient queueClient, string queueName, Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue, 
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ConsumerCountManager consumerCountManager = null, 
            IMessageRejectionHandler messageRejectionHandler = null, bool autoStartup = true)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler, autoStartup)
        {
            _callbackAction = callbackAction;
        }

        protected override bool TryInvoke(T message, List<Exception> exceptions)
        {
            try
            {
                _callbackAction(message);

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
