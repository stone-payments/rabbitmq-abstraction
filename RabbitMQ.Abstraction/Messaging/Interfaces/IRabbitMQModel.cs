using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IRabbitMQModel : IModel
    {
        void End();
    }
}
