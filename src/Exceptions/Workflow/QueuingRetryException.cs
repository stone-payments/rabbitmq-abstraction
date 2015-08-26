using System;

namespace Vtex.RabbitMQ.Exceptions.Workflow
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
