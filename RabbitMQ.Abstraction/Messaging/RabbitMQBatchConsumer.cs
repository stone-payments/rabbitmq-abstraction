using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQBatchConsumer<T> : AbstractRabbitMQConsumer where T : class
    {
        private readonly IBatchProcessingWorker<T> _batchProcessingWorker;

        public RabbitMQBatchConsumer(RabbitMQConnectionPool connectionPool, string queueName, IBatchProcessingWorker<T> batchProcessingWorker, ISerializer serializer = null, ILogger logger = null, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(connectionPool, queueName, serializer, logger, consumerCountManager, messageRejectionHandler)
        {
            _batchProcessingWorker = batchProcessingWorker;
        }

        protected override async Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync()
        {
            var newConsumerWorker = new RabbitMQBatchConsumerWorker<T>(
                connection: await ConnectionPool.GetConnectionAsync().ConfigureAwait(false),
                queueName: QueueName,
                batchProcessingWorker: _batchProcessingWorker,
                messageRejectionHandler: MessageRejectionHandler,
                serializer: Serializer,
                scaleCallbackFunc: TryScaleDown
            );

            return newConsumerWorker;
        }
    }
}
