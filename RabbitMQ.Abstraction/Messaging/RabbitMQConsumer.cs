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
    public class RabbitMQConsumer<T> : IQueueConsumer where T : class
    {
        private readonly RabbitMQConnectionPool _connectionPool;

        private readonly string _queueName;

        private readonly ISerializer _serializer;

        private readonly IErrorLogger _errorLogger;

        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;

        private readonly IMessageRejectionHandler _messageRejectionHandler;

        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly IConsumerCountManager _consumerCountManager;

        private int _scalingAmount;
        private int _consumerWorkersCount;

        private readonly object _scalingLock = new object();

        public RabbitMQConsumer(RabbitMQConnectionPool connectionPool, string queueName, 
            IMessageProcessingWorker<T> messageProcessingWorker, ISerializer serializer = null, IErrorLogger errorLogger = null, 
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
        {
            _connectionPool = connectionPool;
            _queueName = queueName;
            _serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;
            _messageProcessingWorker = messageProcessingWorker;
            _consumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            _messageRejectionHandler = messageRejectionHandler ?? new MessageDeserializationRejectionHandler(connectionPool);

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
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                return GetMessageCount(model);
            }
        }

        public uint GetConsumerCount()
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
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
                                    "QueueName", _queueName);
                            }
                        }, cancellationToken);
                    }
                }

                await Task.Delay(_consumerCountManager.AutoscaleFrequency, cancellationToken).ConfigureAwait(false);
            }
        }

        private RabbitMQConsumerWorker<T> CreateNewConsumerWorker()
        {
            var newConsumerWorker = new RabbitMQConsumerWorker<T>(
                connection: _connectionPool.GetConnection(),
                queueName: _queueName,
                messageProcessingWorker: _messageProcessingWorker,
                messageRejectionHandler: _messageRejectionHandler,
                serializer: _serializer,
                scaleCallbackFunc: TryScaleDown
            );

            return newConsumerWorker;
        }

        private bool TryScaleDown()
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
            Stop();
        }

        private QueueInfo CreateQueueInfo()
        {
            QueueInfo queueInfo;
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var queueDeclareOk = model.QueueDeclarePassive(_queueName);

                queueInfo = new QueueInfo
                {
                    QueueName = _queueName,
                    ConsumerCount = queueDeclareOk.ConsumerCount,
                    MessageCount = queueDeclareOk.MessageCount
                };
            }
            return queueInfo;
        }

        private uint GetMessageCount(IModel model)
        {
            var queueDeclareOk = model.QueueDeclarePassive(_queueName);
            return queueDeclareOk.MessageCount;
        }

        private uint GetConsumerCount(IModel model)
        {
            var queueDeclareOk = model.QueueDeclarePassive(_queueName);
            return queueDeclareOk.ConsumerCount;
        }
    }
}
