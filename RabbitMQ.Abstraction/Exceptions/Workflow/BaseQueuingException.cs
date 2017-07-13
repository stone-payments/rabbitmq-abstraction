using System;

namespace RabbitMQ.Abstraction.Exceptions.Workflow
{
    public class BaseQueuingException : Exception
    {
        public BaseQueuingException()
        {
        }

        public BaseQueuingException(string message)
            : base(message)
        {
        }

        public BaseQueuingException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
