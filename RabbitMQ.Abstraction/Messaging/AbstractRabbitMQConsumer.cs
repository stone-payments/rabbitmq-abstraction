using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Logging.Interfaces;
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

        private readonly IErrorLogger _errorLogger;

        protected readonly IMessageRejectionHandler MessageRejectionHandler;

        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly IConsumerCountManager _consumerCountManager;

        private int _scalingAmount;
        private int _consumerWorkersCount;

        private readonly object _scalingLock = new object();

        protected AbstractRabbitMQConsumer(RabbitMQConnectionPool connectionPool, string queueName, 
            ISerializer serializer = null, IErrorLogger errorLogger = null, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            ConnectionPool = connectionPool;
            QueueName = queueName;
            Serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;
            _consumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler = messageRejectionHandler ?? new MessageDeserializationRejectionHandler(connectionPool);

            _consumerWorkersCount = 0;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public Task StartAsync(CancellationToken cancellationToken)
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
                await Task.Delay(1);
            }
        }

        public uint GetMessageCount()
        {
            using (var model = ConnectionPool.GetConnection().CreateModel())
            {
                return GetMessageCount(model);
            }
        }

        public uint GetConsumerCount()
        {
            using (var model = ConnectionPool.GetConnection().CreateModel())
            {
                return GetConsumerCount(model);
            }
        }

        protected virtual async Task ManageConsumersLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var queueInfo = CreateQueueInfo();

                lock (_scalingLock)
                {
                    _scalingAmount = _consumerCountManager.GetScalingAmount(queueInfo, _consumerWorkersCount);

                    for (var i = 1; i <= _scalingAmount; i++)
                    {
                        Task.Factory.StartNew(async () =>
                        {
                            try
                            {
                                Interlocked.Decrement(ref _scalingAmount);
                                Interlocked.Increment(ref _consumerWorkersCount);

                                using (IQueueConsumerWorker consumerWorker = CreateNewConsumerWorker())
                                {
                                    await consumerWorker.DoConsumeAsync(cancellationToken).ConfigureAwait(false);
                                }
                            }
                            catch (Exception exception)
                            {
                                Interlocked.Increment(ref _scalingAmount);
                                Interlocked.Decrement(ref _consumerWorkersCount);

                                _errorLogger?.LogError("RabbitMQ.AdvancedConsumer", exception.ToString(),
                                    "QueueName", QueueName);
                            }
                        }, cancellationToken);
                    }
                }

                await Task.Delay(_consumerCountManager.AutoscaleFrequency, cancellationToken).ConfigureAwait(false);
            }
        }

        protected abstract IQueueConsumerWorker CreateNewConsumerWorker();
        
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

        private QueueInfo CreateQueueInfo()
        {
            QueueInfo queueInfo;
            using (var model = ConnectionPool.GetConnection().CreateModel())
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
