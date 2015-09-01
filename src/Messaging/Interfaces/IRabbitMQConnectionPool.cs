using System;
using RabbitMQ.Client;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IRabbitMQConnectionPool : IDisposable
    {
        IConnection GetConnection();
    }
}
