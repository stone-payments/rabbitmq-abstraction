using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public abstract class AbstractRabbitMQFeedbackSender : IFeedbackSender
    {
        protected readonly IModel Model;

        protected ulong DeliveryTag { get; }

        public bool HasAcknoledged { get; private set; }

        protected AbstractRabbitMQFeedbackSender(IModel model, ulong deliveryTag)
        {
            Model = model;
            DeliveryTag = deliveryTag;

            HasAcknoledged = false;
        }

        public void Ack()
        {
            if (!HasAcknoledged)
            {
                Model.BasicAck(DeliveryTag, IsMulti());

                HasAcknoledged = true;
            }
        }

        public void Nack(bool requeue)
        {
            if (!HasAcknoledged)
            {
                Model.BasicNack(DeliveryTag, IsMulti(), requeue);

                HasAcknoledged = true;
            }
        }

        protected abstract bool IsMulti();
    }
}
