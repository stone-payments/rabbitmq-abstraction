using Newtonsoft.Json;

namespace RabbitMQ.Abstraction
{
    public class VirtualHostUserPermission
    {
        [JsonProperty(PropertyName = "configure")]
        public string Configure { get; set; }

        [JsonProperty(PropertyName = "write")]
        public string Write { get; set; }

        [JsonProperty(PropertyName = "read")]
        public string Read { get; set; }
    }
}
