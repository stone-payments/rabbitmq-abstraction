using Newtonsoft.Json;
using Vtex.RabbitMQ.Serialization.Interfaces;

namespace Vtex.RabbitMQ.Serialization
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
