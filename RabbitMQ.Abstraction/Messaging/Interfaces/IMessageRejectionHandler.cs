using System.Threading.Tasks;
using RabbitMQ.Abstraction.Exceptions;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IMessageRejectionHandler
    {
        Task OnRejectionAsync(RejectionException exception);
    }
}
