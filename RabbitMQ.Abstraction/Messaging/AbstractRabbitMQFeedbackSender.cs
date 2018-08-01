using System;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQ.Abstraction.Messaging
{
    public abstract class AbstractRabbitMQFeedbackSender : IFeedbackSender
    {
        protected readonly IModel Model;

        protected readonly ISubscription Subscription;

        protected BasicDeliverEventArgs BasicDeliverEventArgs { get; }

        protected ulong DeliveryTag { get; }

        public bool HasAcknoledged { get; private set; }

        protected AbstractRabbitMQFeedbackSender(IModel model, ulong deliveryTag)
        {
            Model = model;
            DeliveryTag = deliveryTag;

            HasAcknoledged = false;
        }

        protected AbstractRabbitMQFeedbackSender(ISubscription subscription,
            BasicDeliverEventArgs basicDeliverEventArgs)
        {
            Subscription = subscription;
            BasicDeliverEventArgs = basicDeliverEventArgs;
        }

        public void Ack()
        {
            if (!HasAcknoledged)
            {
                if (IsMulti() && Model == null)
                {
                    throw new InvalidOperationException(
                        $"Unable to perform multi acknoledgement when using Subscription");
                }

                if (IsMulti() || Model != null)
                {
                    Model.BasicAck(DeliveryTag, IsMulti());
                }
                else 
                {
                    Subscription.Ack(BasicDeliverEventArgs);
                }

                HasAcknoledged = true;
            }
        }

        public void Nack(bool requeue)
        {
            if (!HasAcknoledged)
            {
                if (Model != null)
                {
                    Model.BasicNack(DeliveryTag, IsMulti(), requeue);
                }
                else
                {
                    Subscription.Nack(BasicDeliverEventArgs, IsMulti(), requeue);
                }

                HasAcknoledged = true;
            }
        }

        protected abstract bool IsMulti();
    }
}
