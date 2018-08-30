using System;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Exceptions;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;

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
            _rejectionRoutingKey = exchangeName == "" ? DefaultRejectionQueueName : rejectionRoutingKey;
            _serializer = serializer ?? new JsonSerializer();
        }

        public MessageDeserializationRejectionHandler(ConnectionFactory connectionFactory, string exchangeName = "",
            string rejectionRoutingKey = "RejectedMessages", ISerializer serializer = null)
        {
            _rabbitMQClient = new RabbitMQClient(connectionFactory, serializer);
            _exchangeName = exchangeName;
            _rejectionRoutingKey = exchangeName == "" ? DefaultRejectionQueueName : rejectionRoutingKey;
            _serializer = serializer ?? new JsonSerializer();
        }

        public async Task OnRejectionAsync(RejectionException exception)
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

            await EnsureQueueAndBindingAsync().ConfigureAwait(false);

            await _rabbitMQClient.PublishAsync(_exchangeName, _rejectionRoutingKey, message).ConfigureAwait(false);
        }

        private async Task EnsureQueueAndBindingAsync()
        {
            await _rabbitMQClient.QueueDeclareAsync(DefaultRejectionQueueName).ConfigureAwait(false);

            if (_exchangeName != "")
            {
                await _rabbitMQClient.ExchangeDeclareAsync(_exchangeName).ConfigureAwait(false);

                await _rabbitMQClient.QueueBindAsync(DefaultRejectionQueueName, _exchangeName, _rejectionRoutingKey).ConfigureAwait(false);
            }
        }
    }
}
