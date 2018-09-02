using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQBatchConsumer<T> : AbstractRabbitMQConsumer where T : class
    {
        private readonly IBatchProcessingWorker<T> _batchProcessingWorker;
        private readonly ILogger _logger;

        public RabbitMQBatchConsumer(ILogger logger, ConnectionFactory connectionFactory, RabbitMQConnection connection, RabbitMQConnection connectionPublisher, string queueName, IBatchProcessingWorker<T> batchProcessingWorker, ISerializer serializer = null, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(connectionFactory, connection, connectionPublisher, queueName, serializer, logger, consumerCountManager, messageRejectionHandler)
        {
            _batchProcessingWorker = batchProcessingWorker;
            _logger = logger;
        }

        protected override async Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync()
        {
            var newConsumerWorker = new RabbitMQBatchConsumerWorker<T>(
                logger: _logger,
                connection: ConnectionConsumer,
                model: ConnectionConsumer.CreateModel(true).Model,
                queueName: QueueName,
                batchProcessingWorker: _batchProcessingWorker,
                messageRejectionHandler: MessageRejectionHandler,
                serializer: Serializer
            );

            return newConsumerWorker;
        }
    }
}
