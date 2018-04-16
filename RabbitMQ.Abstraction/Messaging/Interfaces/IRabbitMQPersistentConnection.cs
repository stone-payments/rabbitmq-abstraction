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

        string UserName { get; set; }

        string Password { get; set; }

        string HostName { get; set; }

        int Port { get; set; }
    }
}