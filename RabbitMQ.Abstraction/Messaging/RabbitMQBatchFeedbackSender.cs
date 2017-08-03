using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQBatchFeedbackSender : AbstractRabbitMQFeedbackSender
    {
        public RabbitMQBatchFeedbackSender(IModel model, ulong deliveryTag) : base(model, deliveryTag)
        {
        }

        protected override bool IsMulti()
        {
            return true;
        }
    }
}
