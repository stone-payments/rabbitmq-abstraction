using System;
using System.Collections.Generic;
using RabbitMQ.Abstraction.Interfaces;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueClient : IDisposable
    {
        void Publish<T>(string exchangeName, string routingKey, T content);

        void BatchPublish<T>(string exchangeName, string routingKey, IEnumerable<T> contentList);

        void BatchPublishTransactional<T>(string exchangeName, string routingKey, IEnumerable<T> contentList);

        void DelayedPublish<T>(string exchangeName, string routingKey, T content, TimeSpan delay);

        IQueueConsumer GetConsumer<T>(string queueName, IConsumerCountManager consumerCountManager, 
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler) 
            where T : class;

        void QueueDeclare(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false, 
            IDictionary<string, object> arguments = null);

        void QueueDeclarePassive(string queueName);

        uint QueueDelete(string queueName);

        void QueueBind(string queueName, string exchangeName, string routingKey);

        uint QueuePurge(string queueName);

        void ExchangeDeclare(string exchangeName, bool passive = false);

        bool QueueExists(string queueName);

        void EnsureQueueExists(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false, 
            IDictionary<string, object> arguments = null);

        uint GetMessageCount(string queueName);

        uint GetConsumerCount(string queueName);
    }
}
