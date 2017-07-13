using System;

namespace RabbitMQ.Abstraction.Exceptions
{
    [Serializable]
    public class DeserializationException : RejectionException
    {
        public DeserializationException()
        {
        }

        public DeserializationException(string message)
            : base(message)
        {
        }

        public DeserializationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public string SerializedDataString { get; set; }

        public byte[] SerializedDataBinary { get; set; }
    }
}
