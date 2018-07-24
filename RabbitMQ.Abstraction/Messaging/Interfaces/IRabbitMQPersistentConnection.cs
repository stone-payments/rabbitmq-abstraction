using RabbitMQ.Client;
using System;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IRabbitMQPersistentConnection : IDisposable
    {
        bool IsConnected { get; }

        void Initialize();

        IModel CreateModel();

        IConnection connection { get; set; }

        ConnectionFactory ConnectionFactory { get; set; }
    }
}