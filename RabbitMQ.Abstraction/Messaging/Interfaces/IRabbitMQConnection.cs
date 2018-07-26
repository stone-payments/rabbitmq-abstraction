using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging.Interfaces
{
    public interface IRabbitMQConnection : IConnection
    {
        Task<IModel> GetModelAsync();
    }
}
