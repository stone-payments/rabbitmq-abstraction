using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQMessageFeedbackSender : AbstractRabbitMQFeedbackSender
    {
        public RabbitMQMessageFeedbackSender(IModel model, ulong deliveryTag) : base(model, deliveryTag)
        {
        }

        public RabbitMQMessageFeedbackSender(ISubscription subscription, BasicDeliverEventArgs basicDeliverEventArgs) :
            base(subscription, basicDeliverEventArgs)
        {
        }

        protected override bool IsMulti()
        {
            return false;
        }
    }
}
