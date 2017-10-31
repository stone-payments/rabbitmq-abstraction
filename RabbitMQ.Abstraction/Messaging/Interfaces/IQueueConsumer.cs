using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueConsumer : IDisposable
    {
        Task<Task> StartAsync(CancellationToken cancellationToken);

        Task Stop();

        uint GetMessageCount();

        uint GetConsumerCount();
    }
}
