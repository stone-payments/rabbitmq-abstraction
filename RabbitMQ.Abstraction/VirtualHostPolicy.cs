using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Abstraction
{
    public class VirtualHostPolicy
    {
        public string Pattern { get; }

        public Dictionary<string, string> Definition { get; }

        public byte Priority { get; }

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
