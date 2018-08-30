using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        Task StartAsync(CancellationToken cancellationToken);

        void Stop();
    }
}