using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConsumerContext
    {
        private readonly IModel _model;
        private readonly IQueueClient _queueClient;

        public RabbitMQConsumerContext(IQueueClient queueClient, IModel model)
        {
            _model = model;
            _queueClient = queueClient;
        }

        public Task PublishAsync<T>(string exchangeName, string routingKey, T content, byte? priority = null, TimeSpan? delay = null)
        {
            return _queueClient.PublishAsync(_model, exchangeName, routingKey, content, priority, delay);
        }

        public Task BatchPublishAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList,
            byte? priority = null)
        {
            return _queueClient.BatchPublishAsync(_model, exchangeName, routingKey, contentList, priority);
        }
    }
}