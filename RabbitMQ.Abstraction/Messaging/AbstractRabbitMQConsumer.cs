using System;
using System.Collections.Concurrent;
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

        protected IRabbitMQConnection ConsumerConnection
        {
            get
            {
                if (_consumerConnection != null)
                {
                    if (_consumerConnection.IsOpen)
                    {
                        return _consumerConnection;
                    }
                }

                lock(_consumerConnectionLock)
                {
                    if (_consumerConnection != null)
                    {
                        if (_consumerConnection.IsOpen)
                        {
                            return _consumerConnection;
                        }

                        UnsubscribeConnectionEvents();
                        _consumerConnection.Dispose();
                        _consumerConnection = null;
                    }

                    var success = false;

                    do
                    {
                        try
                        {
                            _consumerConnection =
                                ConnectionPool.CreateConnection(ConsumerCountManager.MaxConcurrentConsumers * 2, QueueName);

                            SubscribeConnectionEvents();
                            success = true;
                        }
                        catch (Exception e)
                        {
                            _logger?.LogError(e, $"Unable to create consumer connection");

                            Task.Delay(TimeSpan.FromMilliseconds(50)).Wait();
                        }
                    } while (!success);


                    return _consumerConnection;
                }
            }
        }

        private void SubscribeConnectionEvents()
        {
            _consumerConnection.ConnectionShutdown += _consumerConnection_ConnectionShutdown;
            _consumerConnection.ConnectionRecoveryError += _consumerConnection_ConnectionRecoveryError;
            _consumerConnection.CallbackException += _consumerConnection_CallbackException;
            _consumerConnection.ConnectionBlocked += _consumerConnection_ConnectionBlocked;
            _consumerConnection.ConnectionUnblocked += _consumerConnection_ConnectionUnblocked;
            _consumerConnection.RecoverySucceeded += _consumerConnection_RecoverySucceeded;
        }

        private void UnsubscribeConnectionEvents()
        {
            _consumerConnection.ConnectionShutdown -= _consumerConnection_ConnectionShutdown;
            _consumerConnection.ConnectionRecoveryError -= _consumerConnection_ConnectionRecoveryError;
            _consumerConnection.CallbackException -= _consumerConnection_CallbackException;
            _consumerConnection.ConnectionBlocked -= _consumerConnection_ConnectionBlocked;
            _consumerConnection.ConnectionUnblocked -= _consumerConnection_ConnectionUnblocked;
            _consumerConnection.RecoverySucceeded -= _consumerConnection_RecoverySucceeded;
        }

        private void _consumerConnection_RecoverySucceeded(object sender, EventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{QueueName}] Connection Recovery Succeeded");
        }

        private void _consumerConnection_ConnectionUnblocked(object sender, EventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{QueueName}] Connection Unblocked");
        }

        private void _consumerConnection_ConnectionBlocked(object sender, Client.Events.ConnectionBlockedEventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{QueueName}] Connection Blocked");
        }

        private void _consumerConnection_CallbackException(object sender, Client.Events.CallbackExceptionEventArgs e)
        {
            _logger.LogError(e.Exception, $"RabbitMQAbstraction[{QueueName}] Connection CallbackException. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void _consumerConnection_ConnectionRecoveryError(object sender, Client.Events.ConnectionRecoveryErrorEventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{QueueName}] Connection Recovery Error. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void _consumerConnection_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{QueueName}] Connection Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText}");
        }

        private IRabbitMQConnection _consumerConnection;
        private readonly object _consumerConnectionLock =  new object();

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

            _consumerConnection.Dispose();
        }

        public async Task<uint> GetMessageCountAsync()
        {
            using (var model = await (await ConnectionPool.GetConnectionAsync().ConfigureAwait(false)).GetModelAsync().ConfigureAwait(false))
            {
                return GetMessageCount(model);
            }
        }

        public async Task<uint> GetConsumerCountAsync()
        {
            using (var model = await (await ConnectionPool.GetConnectionAsync().ConfigureAwait(false)).GetModelAsync().ConfigureAwait(false))
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

                            var consumerWorkerStartTasks = new ConcurrentBag<Task>();
                            
                            Parallel.For(0, counter, i =>
                            {
                                consumerWorkerStartTasks.Add(Task.Factory.StartNew(async () =>
                                {
                                    try
                                    {
                                        Interlocked.Decrement(ref _scalingAmount);
                                        Interlocked.Increment(ref _consumerWorkersCount);

                                        var consumerWorker = await CreateNewConsumerWorkerAsync().ConfigureAwait(false);

                                        await consumerWorker.DoConsumeAsync(cancellationToken).ConfigureAwait(false);
                                    }
                                    catch (Exception exception)
                                    {
                                        Interlocked.Increment(ref _scalingAmount);
                                        Interlocked.Decrement(ref _consumerWorkersCount);

                                        _logger?.LogError(exception,
                                            $"{exception.Message}{Environment.NewLine}{exception.StackTrace}",
                                            new Dictionary<string, string>
                                            {
                                                {"RabbitMQ.AdvancedConsumer", exception.ToString()},
                                                {"QueueName", QueueName}
                                            });
                                    }
                                }, cancellationToken));
                            });

                            var localCancelationToken =
                                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                            localCancelationToken.CancelAfter(
                                (int) ConsumerCountManager.AutoscaleFrequency.TotalMilliseconds +
                                (int) TimeSpan.FromSeconds(30).TotalMilliseconds);

                            Task.WaitAll(consumerWorkerStartTasks.ToArray(), localCancelationToken.Token);
                        }

                        await Task.Delay(ConsumerCountManager.AutoscaleFrequency, cancellationToken)
                            .ConfigureAwait(false);
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
                        if (_consumerWorkersCount > 0)
                        {
                            Interlocked.Increment(ref _scalingAmount);
                            Interlocked.Decrement(ref _consumerWorkersCount);
                        }

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
