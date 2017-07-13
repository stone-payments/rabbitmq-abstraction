using System;

namespace RabbitMQ.Abstraction.Exceptions.Workflow
{
    public class QueuingRetryException : BaseQueuingException
    {
        public QueuingRetryException()
        {
        }

        public QueuingRetryException(string message)
            : base(message)
        {
        }

        public QueuingRetryException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
