using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging
{
    public abstract class AbstractRabbitMQConsumer : IQueueConsumer
    {
        protected readonly RabbitMQConnection ConnectionConsumer;

        protected readonly RabbitMQConnection ConnectionPublisher;

        protected readonly ConnectionFactory _connectionFactory;

        protected readonly string QueueName;

        protected readonly ISerializer Serializer;

        private readonly ILogger _logger;

        protected readonly IMessageRejectionHandler MessageRejectionHandler;

        private readonly CancellationTokenSource _cancellationTokenSource;

        protected readonly IConsumerCountManager ConsumerCountManager;

        protected readonly ushort PrefetchCount;

        protected AbstractRabbitMQConsumer(
            ConnectionFactory connectionFactory,
            RabbitMQConnection connectionConsumer,
            RabbitMQConnection connectionPublisher,
            string queueName,
            ISerializer serializer = null,
            ILogger logger = null,
            IConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null,
            ushort prefetchCount = 1)
        {
            _connectionFactory = connectionFactory;
            ConnectionConsumer = connectionConsumer;
            ConnectionPublisher = connectionPublisher;

            QueueName = queueName;
            Serializer = serializer ?? new JsonSerializer();
            _logger = logger;
            ConsumerCountManager = consumerCountManager ?? new ConsumerCountManager();
            MessageRejectionHandler =
                messageRejectionHandler ?? new MessageDeserializationRejectionHandler(connectionFactory);

            PrefetchCount = prefetchCount;

            _cancellationTokenSource = new CancellationTokenSource();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var token = _cancellationTokenSource.Token;

            return Task.Factory.StartNew(async () => await ManageConsumersLoopAsync(token).ConfigureAwait(false),
                cancellationToken);
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

        protected virtual async Task ManageConsumersLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                //while (!cancellationToken.IsCancellationRequested)
                //{
                //    var queueInfo = CreateQueueInfo(_model);

                    //Console.WriteLine($"Queue: {QueueName} Consumers: {ConsumerCountManager.MaxConcurrentConsumers}");
                    
                    for (var i = 1; i <= ConsumerCountManager.MaxConcurrentConsumers; i++)
                    {
                        //Console.WriteLine($"Queue: {QueueName} creating {i} of {ConsumerCountManager.MaxConcurrentConsumers} consumers");

                        await Task.Factory.StartNew(async () =>
                        {
                            try
                            {
                                using (var consumerWorker = await CreateNewConsumerWorkerAsync().ConfigureAwait(false))
                                {
                                    await consumerWorker.DoConsumeAsync(cancellationToken).ConfigureAwait(false);
                                }

                                //var consumerWorker = await CreateNewConsumerWorkerAsync().ConfigureAwait(false);
                                //await consumerWorker.DoConsumeAsync(cancellationToken).ConfigureAwait(false);
                            }
                            catch (Exception exception)
                            {
                                _logger?.LogError(exception,
                                    $"{exception.Message}{Environment.NewLine}{exception.StackTrace}",
                                    new Dictionary<string, string>
                                    {
                                        {"RabbitMQ.AdvancedConsumer", exception.ToString()},
                                        {"QueueName", QueueName}
                                    });
                            }
                        }, cancellationToken);
                    };

                    await Task.Delay(ConsumerCountManager.AutoscaleFrequency, cancellationToken)
                        .ConfigureAwait(false);
                //}
            }
            catch (Exception e)
            {
                _logger?.LogError(e, $"{e.Message}{Environment.NewLine}{e.StackTrace}");
                throw;
            }
        }

        protected abstract Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync();

        private QueueInfo CreateQueueInfo(IModel model)
        {
            var queueDeclareOk = model.QueueDeclarePassive(QueueName);

            var queueInfo = new QueueInfo
            {
                QueueName = QueueName,
                ConsumerCount = queueDeclareOk.ConsumerCount,
                MessageCount = queueDeclareOk.MessageCount
            };

            return queueInfo;
        }

        public void Dispose()
        {
            Stop();
        }
    }
}