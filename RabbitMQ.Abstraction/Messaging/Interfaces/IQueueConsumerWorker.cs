using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueConsumerWorker : IDisposable
    {
        bool ModelIsClosed { get; }

        Task DoConsumeAsync(CancellationToken cancellationToken);
    }
}
