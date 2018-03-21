using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueClient : IDisposable
    {
        Task PublishAsync<T>(string exchangeName, string routingKey, T content, byte? priority = null);
        Task BatchPublishAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null);
        Task BatchPublishTransactionalAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null);
        Task DelayedPublishAsync<T>(string exchangeName, string routingKey, T content, TimeSpan delay, byte? priority = null);

        Task QueueDeclareAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null);

        Task QueueDeclarePassiveAsync(string queueName);
        Task<uint> QueueDeleteAsync(string queueName);
        Task QueueBindAsync(string queueName, string exchangeName, string routingKey);
        Task ExchangeDeclareAsync(string exchangeName, bool passive = false);
        Task<bool> QueueExistsAsync(string queueName);

        Task EnsureQueueExistsAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null);

        Task<uint> QueuePurgeAsync(string queueName);
        Task<uint> GetMessageCountAsync(string queueName);
        Task<uint> GetConsumerCountAsync(string queueName);
        Task<bool> VirtualHostDeclareAsync(string virtualHostName);
        Task<bool> GrantPermissionsAsync(string virtualHostName, string userName, VirtualHostUserPermission permissions);
        Task<bool> PolicyDeclareAsync(string virtualHostName, string policyName, VirtualHostPolicy policy);

        Task<bool> ShovelDeclareAsync(string virtualHostName, string shovelName,
            ShovelConfiguration shovelConfiguration);

        IQueueConsumer CreateConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class;

        IQueueConsumer CreateBatchConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IBatchProcessingWorker<T> batchProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class;
    }
}
