using System;
using System.Linq;
using Newtonsoft.Json;

namespace RabbitMQ.Abstraction
{
    public class ShovelConfiguration
    {
        [JsonProperty(PropertyName = "value")]
        public ShovelConfigurationContent Value { get; }

        public ShovelConfiguration(ShovelConfigurationContent content)
        {
            Value = content;
        }
    }

    public class ShovelConfigurationContent
    {
        [JsonProperty(PropertyName = "src-uri")]
        public string SourceUri { get; }

        [JsonProperty(PropertyName = "dest-uri")]
        public string DestinationUri { get; }

        [JsonProperty(PropertyName = "src-queue")]
        public string SourceQueue { get; }

        [JsonProperty(PropertyName = "dest-queue")]
        public string DestinationQueue { get; }

        [JsonProperty(PropertyName = "dest-exchange")]
        public string DestinationExchange { get; }

        [JsonProperty(PropertyName = "dest-exchange-key")]
        public string DestinationRoutingKey { get; }

        [JsonProperty(PropertyName = "prefetch-count")]
        public uint PrefetchCount { get; }

        [JsonProperty(PropertyName = "reconnect-delay")]
        public uint ReconnectDelaySeconds { get; }

        [JsonProperty(PropertyName = "add-forward-headers")]
        public bool AddForwardHeaders { get; }

        [JsonProperty(PropertyName = "ack-mode"), JsonConverter(typeof(AckModeJsonConverter))]

        public AckMode AckMode { get; }

        [JsonProperty(PropertyName = "delete-after"), JsonConverter(typeof(DeleteAfterJsonConverter))]
        public DeleteAfter DeleteAfter { get; }

        private ShovelConfigurationContent(string sourceUri, string sourceQueue, string destinationUri,
            string destinationQueue = "", string destinationExchange = "", string destinationRoutingKey = "",
            AckMode ackMode = null, DeleteAfter deleteAfter = null, uint prefetchCount = 1000,
            uint reconnectDelaySeconds = 1, bool addForwardHeaders = false)
        {
            SourceUri = sourceUri;
            SourceQueue = sourceQueue;
            DestinationUri = destinationUri;

            if (!string.IsNullOrWhiteSpace(destinationQueue))
            {
                DestinationQueue = destinationQueue;
            }
            else
            {
                DestinationExchange = destinationExchange;

                if (!string.IsNullOrWhiteSpace(destinationRoutingKey))
                {
                    DestinationRoutingKey = destinationRoutingKey;
                }
            }

            AckMode = ackMode ?? AckMode.OnConfirm;
            DeleteAfter = deleteAfter ?? DeleteAfter.Never;

            PrefetchCount = prefetchCount;
            ReconnectDelaySeconds = reconnectDelaySeconds;
            AddForwardHeaders = addForwardHeaders;
        }

        public ShovelConfigurationContent(string sourceUri, string sourceQueue, string destinationUri,
            string destinationQueue, AckMode ackMode = null, DeleteAfter deleteAfter = null, uint prefetchCount = 1000,
            uint reconnectDelaySeconds = 1, bool addForwardHeaders = false) : this(sourceUri, sourceQueue,
            destinationUri, destinationQueue, "", "", ackMode, deleteAfter, prefetchCount,
            reconnectDelaySeconds, addForwardHeaders)
        {
        }

        public ShovelConfigurationContent(string sourceUri, string sourceQueue, string destinationUri,
            string destinationExchange, string destinationRoutingKey, AckMode ackMode = null, DeleteAfter deleteAfter = null, uint prefetchCount = 1000,
            uint reconnectDelaySeconds = 1, bool addForwardHeaders = false) : this(sourceUri, sourceQueue,
            destinationUri, "", destinationExchange, destinationRoutingKey, ackMode, deleteAfter, prefetchCount,
            reconnectDelaySeconds, addForwardHeaders)
        {
        }
    }

    public sealed class AckMode
    {
        private readonly string _name;

        public static readonly AckMode OnConfirm = new AckMode("on-confirm");
        public static readonly AckMode OnPublish = new AckMode("on-publish");
        public static readonly AckMode NoAck = new AckMode("no-ack");

        private AckMode(string name)
        {
            this._name = name;
        }

        public override string ToString()
        {
            return _name;
        }

        public static AckMode Parse(string value)
        {
            var acceptedValues = new[] {"on-confirm", "on-publish", "no-ack"};

            return acceptedValues.Contains(value) ? new AckMode(value) : null;
        }
    }

    public sealed class AckModeJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteRawValue($"\"{(AckMode)value}\"");
            writer.Flush();
        }

        public override bool CanRead => true;

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return reader.Value != null ? AckMode.Parse(reader.Value.ToString()) : null;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(AckMode);
        }
    }

    public sealed class DeleteAfter
    {
        private readonly string _name;

        public static readonly DeleteAfter Never = new DeleteAfter("never");
        public static readonly DeleteAfter QueueLength = new DeleteAfter("queue-length");

        public static DeleteAfter Amount(uint amount)
        {
            return new DeleteAfter(amount.ToString());
        }

        private DeleteAfter(string name)
        {
            this._name = name;
        }

        public override string ToString()
        {
            return _name;
        }

        public static DeleteAfter Parse(string value)
        {
            var acceptedValues = new[] { "never", "queue-length"};

            return acceptedValues.Contains(value) ? new DeleteAfter(value) : null;
        }
    }

    public sealed class DeleteAfterJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteRawValue($"\"{(DeleteAfter)value}\"");
            writer.Flush();
        }

        public override bool CanRead => true;

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return reader.Value != null ? DeleteAfter.Parse(reader.Value.ToString()) : null;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(DeleteAfter);
        }
    }
}
