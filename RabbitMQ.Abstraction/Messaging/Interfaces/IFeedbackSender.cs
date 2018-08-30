namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IFeedbackSender
    {
        bool HasAcknoledged { get; }

        void Ack();

        void Nack(bool requeue);
    }
}