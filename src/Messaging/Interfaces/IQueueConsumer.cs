using System;
using System.Threading;
using System.Threading.Tasks;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        Task StartAsync(CancellationToken cancellationToken);

        void Stop();

        uint GetMessageCount();

        uint GetConsumerCount();
    }
}
