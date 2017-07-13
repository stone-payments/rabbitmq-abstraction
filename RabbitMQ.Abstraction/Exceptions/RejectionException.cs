using System;

namespace RabbitMQ.Abstraction.Exceptions
{
    [Serializable]
    public class RejectionException : Exception
    {
        public RejectionException()
        {
        }

        public RejectionException(string message)
            : base(message)
        {
        }

        public RejectionException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public string VirtualHost { get; set; }

        public string QueueName { get; set; }
    }
}
