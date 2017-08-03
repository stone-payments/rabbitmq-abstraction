using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace RabbitMQ.Abstraction
{
    public class VirtualHostPolicy
    {
        [JsonProperty(PropertyName = "pattern")]
        public string Pattern { get; }

        [JsonProperty(PropertyName = "definition")]
        public Dictionary<string, string> Definition { get; }

        [JsonProperty(PropertyName = "priority")]
        public byte Priority { get; }

        [JsonProperty(PropertyName = "apply-to")]
        public PolicyScope ApplyTo { get; }

        public VirtualHostPolicy(string pattern, Dictionary<string, string> definition, byte priority = 0B0, PolicyScope applyTo = null)
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

    }
}
