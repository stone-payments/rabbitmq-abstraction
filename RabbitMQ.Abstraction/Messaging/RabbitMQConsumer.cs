using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumer<T> : AbstractRabbitMQConsumer where T : class
    {
        private readonly IMessageProcessingWorker<T> _messageProcessingWorker;

        public RabbitMQConsumer(RabbitMQConnectionPool connectionPool, string queueName,
            IMessageProcessingWorker<T> messageProcessingWorker, ISerializer serializer = null, ILogger logger = null,
            IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null)
            : base(connectionPool, queueName, serializer, logger, consumerCountManager, messageRejectionHandler)
        {
            _messageProcessingWorker = messageProcessingWorker;
        }

        protected override async Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync()
        {
            var newConsumerWorker = new RabbitMQConsumerWorker<T>(
                connection: await ConnectionPool.GetConnectionAsync().ConfigureAwait(false),
                queueName: QueueName,
                messageProcessingWorker: _messageProcessingWorker,
                messageRejectionHandler: MessageRejectionHandler,
                serializer: Serializer,
                scaleCallbackFunc: TryScaleDown
            );

            return newConsumerWorker;
        }
    }
}
