using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace RabbitMQ.Abstraction
{
    public class VirtualHostPolicy
    {
        [JsonProperty(PropertyName = "pattern")]
        public string Pattern { get; }

        [JsonProperty(PropertyName = "definition")]
        public Dictionary<string, object> Definition { get; }

        [JsonProperty(PropertyName = "priority")]
        public byte Priority { get; }

        [JsonProperty(PropertyName = "apply-to"), JsonConverter(typeof(PolicyScopeConverter))]
        public PolicyScope ApplyTo { get; }

        public VirtualHostPolicy(string pattern, Dictionary<string, object> definition, byte priority = 0B0, PolicyScope applyTo = null)
        {
            Pattern = pattern;
            Definition = definition;
            Priority = priority;
            ApplyTo = applyTo ?? PolicyScope.All;
        }
    }

    public sealed class PolicyScope
    {
        private readonly string _name;

        public static readonly PolicyScope All = new PolicyScope("all");
        public static readonly PolicyScope Exchanges = new PolicyScope("exchanges");
        public static readonly PolicyScope Queues = new PolicyScope("queues");

        private PolicyScope(string name)
        {
            this._name = name;
        }

        public override string ToString()
        {
            return _name;
        }

        public static PolicyScope Parse(string value)
        {
            var acceptedValues = new[] { "all", "exchanges", "queues" };

            return acceptedValues.Contains(value) ? new PolicyScope(value) : null;
        }
    }

    public sealed class PolicyScopeConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteRawValue($"\"{(PolicyScope)value}\"");
            writer.Flush();
        }

        public override bool CanRead => true;

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return reader.Value != null ? PolicyScope.Parse(reader.Value.ToString()) : null;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(PolicyScope);
        }
    }
}
