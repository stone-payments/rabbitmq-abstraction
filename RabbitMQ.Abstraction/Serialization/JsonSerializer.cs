using Newtonsoft.Json;
using RabbitMQ.Abstraction.Serialization.Interfaces;

namespace RabbitMQ.Abstraction.Serialization
{
    public class JsonSerializer : ISerializer
    {
        public string Serialize<T>(T entity)
        {
            return JsonConvert.SerializeObject(entity);
        }

        public T Deserialize<T>(string serializedEntity)
        {
            return JsonConvert.DeserializeObject<T>(serializedEntity);
        }
    }
}
