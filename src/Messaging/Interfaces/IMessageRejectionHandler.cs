using Vtex.RabbitMQ.Exceptions;

namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IMessageRejectionHandler
    {
        void OnRejection(RejectionException exception);
    }
}
