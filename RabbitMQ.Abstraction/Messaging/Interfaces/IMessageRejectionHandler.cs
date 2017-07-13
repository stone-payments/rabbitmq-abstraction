using RabbitMQ.Abstraction.Exceptions;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IMessageRejectionHandler
    {
        void OnRejection(RejectionException exception);
    }
}
