using System;
using System.Threading.Tasks;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        Task StartAsync();

        void Stop();

        uint GetMessageCount();

        uint GetConsumerCount();
    }
}
