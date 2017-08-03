using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQMessageFeedbackSender : AbstractRabbitMQFeedbackSender
    {
        public RabbitMQMessageFeedbackSender(IModel model, ulong deliveryTag) : base(model, deliveryTag)
        {
        }

        protected override bool IsMulti()
        {
            return false;
        }
    }
}
