using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Exceptions.Workflow;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction.ProcessingWorkers
{
    public abstract class AbstractAdvancedProcessingWorker<T> : AbstractSimpleProcessingWorker<T> where T : class
    {
        protected readonly int InvokeRetryCount;

        protected readonly int InvokeRetryWaitMilliseconds;

        protected readonly ExceptionHandlingStrategy ExceptionHandlingStrategy;

        protected AbstractAdvancedProcessingWorker(IQueueConsumer consumer,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0, ILogger logger = null)
            : base(consumer, logger)
        {
            InvokeRetryCount = invokeRetryCount;
            InvokeRetryWaitMilliseconds = invokeRetryWaitMilliseconds;
            ExceptionHandlingStrategy = exceptionHandlingStrategy;
        }

        protected AbstractAdvancedProcessingWorker(IQueueClient queueClient, string queueName,
            ExceptionHandlingStrategy exceptionHandlingStrategy = ExceptionHandlingStrategy.Requeue,
            int invokeRetryCount = 1, int invokeRetryWaitMilliseconds = 0,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null, ILogger logger = null, ushort prefetchCount = 1)
            : base(queueClient, queueName, consumerCountManager, messageRejectionHandler, logger, prefetchCount)
        {
            InvokeRetryCount = invokeRetryCount;
            InvokeRetryWaitMilliseconds = invokeRetryWaitMilliseconds;
            ExceptionHandlingStrategy = exceptionHandlingStrategy;
        }

        protected abstract Task<bool> TryInvokeAsync(T message, RabbitMQConsumerContext consumerContext, List<Exception> exceptions,
            CancellationToken cancellationToken);

        protected abstract Task<bool> TryInvokeBatchAsync(IEnumerable<T> batch, List<Exception> exceptions,
            CancellationToken cancellationToken);

        public override async Task OnMessageAsync(T message, RabbitMQConsumerContext consumerContext, IFeedbackSender feedbackSender,
            CancellationToken cancellationToken)
        {
            var invocationSuccess = false;
            var exceptions = new List<Exception>();

            var tryCount = 0;

            while (tryCount == 0 || (!invocationSuccess && ShouldRetry(tryCount, exceptions)))
            {
                if (tryCount > 0 && InvokeRetryWaitMilliseconds > 0)
                {
                    await Task.Delay(InvokeRetryWaitMilliseconds, cancellationToken).ConfigureAwait(false);
                }

                tryCount++;

                invocationSuccess = await TryInvokeAsync(message, consumerContext, exceptions, cancellationToken).ConfigureAwait(false);
            }

            if (invocationSuccess)
            {
                feedbackSender.Ack();
            }
            else if (ShouldRequeue(exceptions))
            {
                feedbackSender.Nack(true);

                Logger.LogWarning(new AggregateException(exceptions), $"Message successfully processed, but exceptions were thrown after {tryCount} tries");
            }
            else
            {
                feedbackSender.Nack(false);

                Logger.LogError(new AggregateException(exceptions), $"Unable to successfully process message after {tryCount} tries");
            }
        }

        public override async Task OnBatchAsync(IEnumerable<T> batch, IFeedbackSender feedbackSender,
            CancellationToken cancellationToken)
        {
            var invocationSuccess = false;
            var exceptions = new List<Exception>();

            var tryCount = 0;

            while (tryCount == 0 || (!invocationSuccess && ShouldRetry(tryCount, exceptions)))
            {
                if (tryCount > 0 && InvokeRetryWaitMilliseconds > 0)
                {
                    await Task.Delay(InvokeRetryWaitMilliseconds, cancellationToken).ConfigureAwait(false);
                }

                tryCount++;

                invocationSuccess = await TryInvokeBatchAsync(batch, exceptions, cancellationToken).ConfigureAwait(false);
            }

            if (invocationSuccess)
            {
                feedbackSender.Ack();
            }
            else if (ShouldRequeue(exceptions))
            {
                feedbackSender.Nack(true);
            }
            else
            {
                feedbackSender.Nack(false);
            }
        }

        private static ExceptionHandlingStrategy? GetStrategyByExceptions(List<Exception> exceptions)
        {
            if (exceptions.Any())
            {
                if (exceptions.Last() is QueuingRetryException || exceptions.Last().InnerException is QueuingRetryException)
                {
                    return ExceptionHandlingStrategy.Retry;
                }
                else if (exceptions.Last() is QueuingRequeueException || exceptions.Last().InnerException is QueuingRequeueException)
                {
                    return ExceptionHandlingStrategy.Requeue;
                }
                else if (exceptions.Last() is QueuingDiscardException || exceptions.Last().InnerException is QueuingDiscardException)
                {
                    return ExceptionHandlingStrategy.Discard;
                }
            }

            return null;
        }

        private bool ShouldRetry(int tryCount, List<Exception> exceptions)
        {
            if (tryCount >= InvokeRetryCount)
            {
                return false;
            }

            var strategyByExceptions = GetStrategyByExceptions(exceptions);

            if (strategyByExceptions != null)
            {
                if (strategyByExceptions == ExceptionHandlingStrategy.Retry)
                {
                    return true;
                }
                else if (strategyByExceptions == ExceptionHandlingStrategy.Discard)
                {
                    return false;
                }
            }

            if (ExceptionHandlingStrategy == ExceptionHandlingStrategy.Retry)
            {
                return true;
            }

            return false;
        }

        private bool ShouldRequeue(List<Exception> exceptions)
        {
            var strategyByExceptions = GetStrategyByExceptions(exceptions);

            if (strategyByExceptions != null)
            {
                if (strategyByExceptions == ExceptionHandlingStrategy.Requeue)
                {
                    return true;
                }
                else if (strategyByExceptions == ExceptionHandlingStrategy.Discard)
                {
                    return false;
                }
            }

            if (ExceptionHandlingStrategy == ExceptionHandlingStrategy.Requeue)
            {
                return true;
            }

            return false;
        }
    }
}
