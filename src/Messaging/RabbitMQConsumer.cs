using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Vtex.RabbitMQ.Interfaces;
using Vtex.RabbitMQ.Logging.Interfaces;
using Vtex.RabbitMQ.Messaging.Interfaces;
using Vtex.RabbitMQ.Serialization;
using Vtex.RabbitMQ.Serialization.Interfaces;

namespace Vtex.RabbitMQ.Messaging
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

        private bool _isStopped;

        private volatile int _scalingAmount;
        private volatile int _consumerWorkersCount;

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
            _isStopped = true;
        }

        public async Task StartAsync()
        {
            _isStopped = false;
            var token = _cancellationTokenSource.Token;
            await Task.Factory.StartNew(async () => await ManageConsumersLoopAsync(token), token);
        }

        public void Stop()
        {
            _isStopped = true;
            //TODO: Review this lock
            //lock (this._scalingAmountSyncLock)
            {
                _scalingAmount = _consumerWorkersCount * -1;
            }
            _cancellationTokenSource.Cancel();
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

        protected async virtual Task ManageConsumersLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!_isStopped)
                {
                    var queueInfo = CreateQueueInfo();
                    _scalingAmount = _consumerCountManager.GetScalingAmount(queueInfo, _consumerWorkersCount);
                    var scalingAmount = _scalingAmount;
                    for (var i = 1; i <= scalingAmount; i++)
                    {
                        _scalingAmount--;
                        _consumerWorkersCount++;

                        await Task.Factory.StartNew(async () =>
                        {
                            try
                            {
                                using (IQueueConsumerWorker consumerWorker = CreateNewConsumerWorker(cancellationToken))
                                {
                                    await consumerWorker.DoConsumeAsync();
                                }
                            }
                            catch (Exception exception)
                            {
                                _errorLogger?.LogError("RabbitMQ.AdvancedConsumer", exception.ToString(),
                                    "QueueName", _queueName);
                            }
                            finally
                            {
                                _consumerWorkersCount--;
                                _scalingAmount++;
                            }
                        }
                        , cancellationToken);
                    }
                }

                await Task.Delay(_consumerCountManager.AutoscaleFrequency, cancellationToken);
            }
        }

        private RabbitMQConsumerWorker<T> CreateNewConsumerWorker(CancellationToken cancellationToken)
        {
            var newConsumerWorker = new RabbitMQConsumerWorker<T>(
                connection: _connectionPool.GetConnection(),
                queueName: _queueName,
                messageProcessingWorker: _messageProcessingWorker,
                messageRejectionHandler: _messageRejectionHandler,
                serializer: _serializer,
                scaleCallbackFunc: GetScalingAmount,
                cancellationToken: cancellationToken
            );

            return newConsumerWorker;
        }

        private int GetScalingAmount()
        {
            return _scalingAmount;
        }

        public void Dispose()
        {
            Stop();
        }

        private QueueInfo CreateQueueInfo()
        {
            QueueInfo queueInfo = null;
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                queueInfo = new QueueInfo
                {
                    QueueName = _queueName,
                    ConsumerCount = GetConsumerCount(model),
                    MessageCount = GetMessageCount(model)
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
