using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IBatchProcessingWorker<in T> where T : class
    {
        Task OnBatchAsync(IEnumerable<T> message, IFeedbackSender feedbackSender, CancellationToken cancellationToken);

        ushort GetBatchSize();
    }
}
