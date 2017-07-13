using System;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization;
using RabbitMQ.Abstraction.Serialization.Interfaces;

namespace RabbitMQ.Abstraction
{
    public class MessageDeserializationRejectionHandler : IMessageRejectionHandler
    {
        private readonly IQueueClient _rabbitMQClient;

        private readonly string _exchangeName;

        private readonly string _rejectionRoutingKey;

        private readonly ISerializer _serializer;

        private const string DefaultRejectionQueueName = "RejectedMessages";

        public MessageDeserializationRejectionHandler(IQueueClient rabbitMQClient, string exchangeName = "",
            string rejectionRoutingKey = "RejectedMessages", ISerializer serializer = null)
        {
            _rabbitMQClient = rabbitMQClient;
            _exchangeName = exchangeName;
            _rejectionRoutingKey = rejectionRoutingKey;
            _serializer = serializer ?? new JsonSerializer();
        }

        public MessageDeserializationRejectionHandler(RabbitMQConnectionPool connectionPool, string exchangeName = "",
            string rejectionRoutingKey = "RejectedMessages", ISerializer serializer = null)
        {
            _rabbitMQClient = new RabbitMQClient(connectionPool, serializer);
            _exchangeName = exchangeName;
            _rejectionRoutingKey = rejectionRoutingKey;
            _serializer = serializer ?? new JsonSerializer();
        }

        public void OnRejection(RejectionException exception)
        {
            var deserializationException = (DeserializationException)exception;

            var message = new DeserializationRejectionMessage
            {
                Date = DateTime.Now,
                QueueName = deserializationException.QueueName,
                SerializedDataBinary = deserializationException.SerializedDataBinary,
                SerializedDataString = deserializationException.SerializedDataString,
                SerializedException = _serializer.Serialize(deserializationException)
            };

            EnsureQueueAndBinding();

            _rabbitMQClient.Publish(_exchangeName, _rejectionRoutingKey, message);
        }

        private void EnsureQueueAndBinding()
        {
            _rabbitMQClient.QueueDeclare(DefaultRejectionQueueName);

            _rabbitMQClient.ExchangeDeclare(_exchangeName);

            _rabbitMQClient.QueueBind(DefaultRejectionQueueName, _exchangeName, _rejectionRoutingKey);
        }
    }
}
