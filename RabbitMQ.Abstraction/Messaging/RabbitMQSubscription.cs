using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQSubscription : Subscription, IDisposable
    {
        public RabbitMQSubscription(IModel model, string queueName) : base(model, queueName)
        {
        }

        public RabbitMQSubscription(IModel model, string queueName, bool autoAck) : base(model, queueName, autoAck)
        {
        }

        public RabbitMQSubscription(IModel model, string queueName, bool autoAck, string consumerTag) : base(model, queueName, autoAck, consumerTag)
        {
        }

        public void Dispose()
        {
            Nack(true, true);

            Close();
        }
    }
}
