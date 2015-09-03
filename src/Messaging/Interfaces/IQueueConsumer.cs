using System;
using System.Threading.Tasks;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        Task Start();

        void Stop();

        uint GetMessageCount();

        uint GetConsumerCount();
    }
}
