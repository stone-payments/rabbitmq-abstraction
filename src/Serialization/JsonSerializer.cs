using System;
using Newtonsoft.Json;
using Vtex.RabbitMQ.Serialization.Interfaces;

namespace Vtex.RabbitMQ.Serialization
{
    public class JsonSerializer : ISerializer
    {
        public string Serialize(object entity)
        {
            return Serialize<object>(entity);
        }

        public string Serialize<T>(T entity)
        {
            return JsonConvert.SerializeObject(entity);
        }

        public object Deserialize(string serializedEntity, Type type)
        {
            return JsonConvert.DeserializeObject(serializedEntity, type);
        }

        public T Deserialize<T>(string serializedEntity)
        {
            return JsonConvert.DeserializeObject<T>(serializedEntity);
        }
    }
}
