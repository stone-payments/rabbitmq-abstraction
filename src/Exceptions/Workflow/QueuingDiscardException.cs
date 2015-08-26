using System;

namespace Vtex.RabbitMQ.Exceptions.Workflow
{
    public class QueuingDiscardException : BaseQueuingException
    {
        public QueuingDiscardException()
        {
        }

        public QueuingDiscardException(string message)
            : base(message)
        {
        }

        public QueuingDiscardException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
