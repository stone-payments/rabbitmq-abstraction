namespace Vtex.RabbitMQ.Messaging.Interfaces
{
    public interface IMessageProcessingWorker<in T> where T : class
    {
        void OnMessage(T message, IMessageFeedbackSender feedbackSender);
    }
}
