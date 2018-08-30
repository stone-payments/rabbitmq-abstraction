using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumer<T> : AbstractRabbitMQConsumer where T : class
    {
        private readonly ILogger _logger;
        private readonly IQueueClient _queueClient;
        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;

        public RabbitMQConsumer(
            ILogger logger,
            IQueueClient queueClient,
            ConnectionFactory connectionFactory,
            IConnection connectionConsumer,
            IConnection connectionPublisher,
            string queueName,
            IMessageProcessingWorker<T> messageProcessingWorker,
            ISerializer serializer = null,
            IConsumerCountManager consumerCountManager = null,
            IMessageRejectionHandler messageRejectionHandler = null,
            ushort prefetchCount = 1)
            : base(connectionFactory, connectionConsumer, connectionPublisher, queueName, serializer, logger, consumerCountManager, messageRejectionHandler, prefetchCount)
        {
            _queueClient = queueClient;
            _logger = logger;
            _messageProcessingWorker = messageProcessingWorker;
        }

        protected override Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync()
        {
            var newConsumerWorker = new RabbitMQConsumerWorker<T>(
                logger: _logger,
                queueClient: _queueClient,
                modelConsumer: ConnectionConsumer.CreateModel(),
                modelPublisher: ConnectionPublisher.CreateModel(),
                queueName: QueueName,
                messageProcessingWorker: _messageProcessingWorker,
                messageRejectionHandler: MessageRejectionHandler,
                serializer: Serializer,
                prefetchCount: PrefetchCount
            );

            return Task.FromResult((IQueueConsumerWorker)newConsumerWorker);
        }
    }
}