using System;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IQueueConsumerWorker : IDisposable
    {
        bool ModelIsClosed { get; }

        void DoConsume();

        void Ack(ulong deliveryTag);

        void Nack(ulong deliveryTag, bool requeue = false);
    }
}
