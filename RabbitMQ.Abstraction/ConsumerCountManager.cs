﻿using System;
using RabbitMQ.Abstraction.Interfaces;

namespace RabbitMQ.Abstraction
{
    public class ConsumerCountManager : IConsumerCountManager
    {
        private readonly uint _minConcurrentConsumers;

        private readonly uint _maxConcurrentConsumers;

        public TimeSpan AutoscaleFrequency { get; set; }

        private readonly uint _messagesPerConsumerWorkerRatio;

        public ConsumerCountManager(uint minConcurrentConsumers = 1, uint maxConcurrentConsumers = 10, 
            uint messagesPerConsumerWorkerRatio = 10, double autoscaleFrequencyMilliseconds = 10000)
        {
            _minConcurrentConsumers = minConcurrentConsumers;
            _maxConcurrentConsumers = maxConcurrentConsumers;
            _messagesPerConsumerWorkerRatio = messagesPerConsumerWorkerRatio;

            AutoscaleFrequency = TimeSpan.FromMilliseconds(autoscaleFrequencyMilliseconds);
        }

        public int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount)
        {
            var consumersByRatio = queueInfo.MessageCount / _messagesPerConsumerWorkerRatio;

            int idealConsumerCount;

            if (consumersByRatio < _minConcurrentConsumers)
            {
                idealConsumerCount = Convert.ToInt32(_minConcurrentConsumers);
            }
            else if (consumersByRatio > _maxConcurrentConsumers)
            {
                idealConsumerCount = Convert.ToInt32(_maxConcurrentConsumers);
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
