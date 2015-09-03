﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.ProcessingWorkers
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
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ConsumerCountManager consumerCountManager = null, 
            IMessageRejectionHandler messageRejectionHandler = null)
            : base(queueClient, queueName, exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, 
            consumerCountManager, messageRejectionHandler)
        {
            _callbackAction = callbackAction;
        }

        public async static Task<AdvancedMessageProcessingWorker<T>> CreateAndStart(IQueueConsumer consumer, 
            Action<T> callbackAction, 
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0)
        {
            var instance = new AdvancedMessageProcessingWorker<T>(consumer, callbackAction, exceptionHandlingStrategy, 
                invokeRetryCount, invokeRetryWaitMilliseconds);

            await instance.Start();

            return instance;
        }

        public async static Task<AdvancedMessageProcessingWorker<T>> CreateAndStart(IQueueClient queueClient,
            string queueName, Action<T> callbackAction,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null)
        {
            var instance = new AdvancedMessageProcessingWorker<T>(queueClient, queueName, callbackAction, 
                exceptionHandlingStrategy, invokeRetryCount, invokeRetryWaitMilliseconds, consumerCountManager, 
                messageRejectionHandler);

            await instance.Start();

            return instance;
        }

        protected override Task<bool> TryInvoke(T message, List<Exception> exceptions)
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
