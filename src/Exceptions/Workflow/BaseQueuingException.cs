using System;

namespace Vtex.RabbitMQ.Exceptions.Workflow
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
