using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueConsumerWorker : IDisposable
    {
        Task DoConsumeAsync(CancellationToken cancellationToken);
    }
}
