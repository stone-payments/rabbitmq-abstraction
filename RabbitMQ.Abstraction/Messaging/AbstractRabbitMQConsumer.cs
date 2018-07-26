using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public abstract class AbstractRabbitMQConsumer : IQueueConsumer
    {
        protected readonly RabbitMQConnectionPool ConnectionPool;

        protected readonly string QueueName;

        protected readonly ISerializer Serializer;

        private readonly ILogger _logger;

        protected readonly IMessageRejectionHandler MessageRejectionHandler;

        private readonly CancellationTokenSource _cancellationTokenSource;

        protected readonly IConsumerCountManager ConsumerCountManager;

        protected readonly ushort PrefetchCount;

        private int _scalingAmount;
        private int _consumerWorkersCount;

        private readonly object _scalingLock = new object();

        protected AbstractRabbitMQConsumer(RabbitMQConnectionPool connectionPool, string queueName,
            ISerializer serializer = null, ILogger logger = null,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null,
            ushort prefetchCount = 1)
        {
            ConnectionPool = connectionPool;
            QueueName = queueName;
            Serializer = serializer ?? new JsonSerializer();
            _logger = logger;
            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler =
                messageRejectionHandler ?? new MessageDeserializationRejectionHandler(connectionPool);

            PrefetchCount = prefetchCount;

            _consumerWorkersCount = 0;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public Task<Task> StartAsync(CancellationToken cancellationToken)
        {
            var token = _cancellationTokenSource.Token;

            return Task.Factory.StartNew(async () => await ManageConsumersLoopAsync(token).ConfigureAwait(false),
                cancellationToken);
        }

        public async Task Stop()
        {
            _cancellationTokenSource.Cancel();

            while (_consumerWorkersCount > 0)
            {
                await Task.Delay(100).ConfigureAwait(false);
            }
        }

        public async Task<uint> GetMessageCountAsync()
        {
            using (var model = await (await ConnectionPool.GetConnectionAsync()).GetModelAsync().ConfigureAwait(false))
            {
                return GetMessageCount(model);
            }
        }

        public async Task<uint> GetConsumerCountAsync()
        {
            using (var model = await (await ConnectionPool.GetConnectionAsync()).GetModelAsync().ConfigureAwait(false))
            {
                return GetConsumerCount(model);
            }
        }

        protected virtual async Task ManageConsumersLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var queueInfo = await CreateQueueInfoAsync().ConfigureAwait(false);

                        lock (_scalingLock)
                        {
                            _scalingAmount = ConsumerCountManager.GetScalingAmount(queueInfo, _consumerWorkersCount);
                            int counter = _scalingAmount;
                            for (var i = 1; i <= counter; i++)
                            {
                                Task.Factory.StartNew(async () =>
                                {
                                    try
                                    {
                                        Interlocked.Decrement(ref _scalingAmount);
                                        Interlocked.Increment(ref _consumerWorkersCount);

                                        using (IQueueConsumerWorker consumerWorker = await CreateNewConsumerWorkerAsync().ConfigureAwait(false))
                                        {
                                            await consumerWorker.DoConsumeAsync(cancellationToken).ConfigureAwait(false);
                                        }
                                    }
                                    catch (Exception exception)
                                    {
                                        Interlocked.Increment(ref _scalingAmount);
                                        Interlocked.Decrement(ref _consumerWorkersCount);

                                        _logger?.LogError(exception, $"{exception.Message}{Environment.NewLine}{exception.StackTrace}",
                                            new Dictionary<string, string>
                                                {
                                                    {"RabbitMQ.AdvancedConsumer", exception.ToString()},
                                                    {"QueueName", QueueName}
                                                });
                                    }
                                }, cancellationToken);
                            }
                        }

                        await Task.Delay(ConsumerCountManager.AutoscaleFrequency, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger?.LogError(e, $"{e.Message}{Environment.NewLine}{e.StackTrace}");
                    }
                }

                _logger?.LogWarning("Stopped consumer AutoScaling");
            }
            catch (Exception e)
            {
                _logger?.LogError(e, $"{e.Message}{Environment.NewLine}{e.StackTrace}");
                throw;
            }
        }

        protected abstract Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync();
        
        protected bool TryScaleDown()
        {
            if (_scalingAmount < 0)
            {
                lock (_scalingLock)
                {
                    if (_scalingAmount < 0)
                    {
                        Interlocked.Increment(ref _scalingAmount);
                        Interlocked.Decrement(ref _consumerWorkersCount);

                        return true;
                    }
                }
            }
            
            return false;
        }

        public void Dispose()
        {
            Stop().Wait();
        }

        private async Task<QueueInfo> CreateQueueInfoAsync()
        {
            QueueInfo queueInfo;
            using (var model = await (await ConnectionPool.GetConnectionAsync().ConfigureAwait(false)).GetModelAsync().ConfigureAwait(false))
            {
                var queueDeclareOk = model.QueueDeclarePassive(QueueName);

                queueInfo = new QueueInfo
                {
                    QueueName = QueueName,
                    ConsumerCount = queueDeclareOk.ConsumerCount,
                    MessageCount = queueDeclareOk.MessageCount
                };
            }
            return queueInfo;
        }

        private uint GetMessageCount(IModel model)
        {
            var queueDeclareOk = model.QueueDeclarePassive(QueueName);
            return queueDeclareOk.MessageCount;
        }

        private uint GetConsumerCount(IModel model)
        {
            var queueDeclareOk = model.QueueDeclarePassive(QueueName);
            return queueDeclareOk.ConsumerCount;
        }
    }
}
