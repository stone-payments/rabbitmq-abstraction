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

        public RabbitMQBatchConsumer(IRabbitMQPersistentConnection persistentConnection, string queueName, IBatchProcessingWorker<T> batchProcessingWorker, ISerializer serializer = null, ILogger logger = null, IConsumerCountManager consumerCountManager = null, IMessageRejectionHandler messageRejectionHandler = null) 
            : base(persistentConnection, queueName, serializer, logger, consumerCountManager, messageRejectionHandler)
        {
            _batchProcessingWorker = batchProcessingWorker;
        }

        protected override Task<IQueueConsumerWorker> CreateNewConsumerWorkerAsync()
        {
            var newConsumerWorker = new RabbitMQBatchConsumerWorker<T>(
                connection: PersistentConnection.connection,
                queueName: QueueName,
                batchProcessingWorker: _batchProcessingWorker,
                messageRejectionHandler: MessageRejectionHandler,
                serializer: Serializer,
                scaleCallbackFunc: TryScaleDown
            );

            return Task.FromResult((IQueueConsumerWorker)newConsumerWorker);
        }
    }
}
