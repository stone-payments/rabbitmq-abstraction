using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQMessageFeedbackSender : IMessageFeedbackSender
    {
        private readonly IModel _model;

        public ulong DeliveryTag { get; private set; }

        public bool MessageAcknoledged { get; private set; }

        public RabbitMQMessageFeedbackSender(IModel model, ulong deliveryTag)
        {
            _model = model;
            DeliveryTag = deliveryTag;

            MessageAcknoledged = false;
        }

        public void Ack()
        {
            if (!MessageAcknoledged)
            {
                _model.BasicAck(DeliveryTag, false);

                MessageAcknoledged = true;
            }
        }

        public void Nack(bool requeue)
        {
            if (!MessageAcknoledged)
            {
                _model.BasicNack(DeliveryTag, false, requeue);

                MessageAcknoledged = true;
            }
        }
    }
}
