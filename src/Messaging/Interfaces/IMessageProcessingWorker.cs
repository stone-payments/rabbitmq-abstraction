using System.Threading.Tasks;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IMessageProcessingWorker<in T> where T : class
    {
        Task OnMessage(T message, IMessageFeedbackSender feedbackSender);
    }
}
