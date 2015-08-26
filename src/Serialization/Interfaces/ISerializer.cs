using System;

namespace Vtex.RabbitMQ.Serialization.Interfaces
{
    public interface ISerializer
    {
        string Serialize(object entity);

        string Serialize<T>(T entity);

        object Deserialize(string serializedEntity, Type type);

        T Deserialize<T>(string serializedEntity);
    }
}
