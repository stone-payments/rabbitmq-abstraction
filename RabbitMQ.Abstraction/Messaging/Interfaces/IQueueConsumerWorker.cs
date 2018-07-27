using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IQueueConsumerWorker
    {
        Task DoConsumeAsync(CancellationToken cancellationToken);
    }
}
