using System;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        void Start();

        void Stop();

        uint GetMessageCount();

        uint GetConsumerCount();
    }
}
