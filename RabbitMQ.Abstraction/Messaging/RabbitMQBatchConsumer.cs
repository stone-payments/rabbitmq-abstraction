using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Logging.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQBatchConsumer<T> : AbstractRabbitMQConsumer where T : class
    {
        private readonly IBatchProcessingWorker<T> _batchProcessingWorker;

        public RabbitMQBatchConsumer(RabbitMQConnectionPool connectionPool, string queueName, IBatchProcessingWorker<T> batchProcessingWorker, ISerializer serializer = null, IErrorLogger errorLogger = null, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(connectionPool, queueName, serializer, errorLogger, consumerCountManager, messageRejectionHandler)
        {
            _batchProcessingWorker = batchProcessingWorker;
        }

        protected override IQueueConsumerWorker CreateNewConsumerWorker()
        {
            var newConsumerWorker = new RabbitMQBatchConsumerWorker<T>(
                connection: ConnectionPool.GetConnection(),
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
