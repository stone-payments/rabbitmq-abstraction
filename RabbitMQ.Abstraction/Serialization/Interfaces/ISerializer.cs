namespace RabbitMQ.Abstraction.Serialization.Interfaces
{
    public interface ISerializer
    {
        string Serialize<T>(T entity);

        T Deserialize<T>(string serializedEntity);
    }
}
