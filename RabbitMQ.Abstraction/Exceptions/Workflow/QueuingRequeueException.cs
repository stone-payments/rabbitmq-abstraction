﻿using System;

namespace RabbitMQ.Abstraction.Exceptions.Workflow
{
    public class QueuingRequeueException : BaseQueuingException
    {
        public QueuingRequeueException()
        {
        }

        public QueuingRequeueException(string message)
            : base(message)
        {
        }

        public QueuingRequeueException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
