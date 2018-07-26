using System;
using RabbitMQ.Abstraction.Interfaces;

namespace RabbitMQ.Abstraction
{
    public class ConsumerCountManager : IConsumerCountManager
    {
        public uint MinConcurrentConsumers { get; }

        public uint MaxConcurrentConsumers { get; }

        public TimeSpan AutoscaleFrequency { get; set; }

        private readonly uint _messagesPerConsumerWorkerRatio;

        public ConsumerCountManager(uint minConcurrentConsumers = 1, uint maxConcurrentConsumers = 10, 
            uint messagesPerConsumerWorkerRatio = 10, double autoscaleFrequencyMilliseconds = 10000)
        {
            MinConcurrentConsumers = minConcurrentConsumers;
            MaxConcurrentConsumers = maxConcurrentConsumers;
            _messagesPerConsumerWorkerRatio = messagesPerConsumerWorkerRatio;

            AutoscaleFrequency = TimeSpan.FromMilliseconds(autoscaleFrequencyMilliseconds);
        }

        public int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount)
        {
            var consumersByRatio = queueInfo.MessageCount / _messagesPerConsumerWorkerRatio;

            int idealConsumerCount;

            if (consumersByRatio < MinConcurrentConsumers)
            {
                idealConsumerCount = Convert.ToInt32(MinConcurrentConsumers);
            }
            else if (consumersByRatio > MaxConcurrentConsumers)
            {
                idealConsumerCount = Convert.ToInt32(MaxConcurrentConsumers);
            }
            else
            {
                idealConsumerCount = Convert.ToInt32(consumersByRatio);
            }

            var scalingAmount = idealConsumerCount - consumersRunningCount;

            return scalingAmount;
        }
    }
}
