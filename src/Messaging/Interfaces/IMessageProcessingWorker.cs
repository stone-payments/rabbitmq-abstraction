using System.Threading.Tasks;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IMessageProcessingWorker<in T> where T : class
    {
        Task OnMessageAsync(T message, IMessageFeedbackSender feedbackSender);
    }
}
