using System;

namespace RabbitMQ.Abstraction.Interfaces
{
    public interface IConsumerCountManager
    {
        TimeSpan AutoscaleFrequency { get; set; }

        uint MinConcurrentConsumers { get; }

        uint MaxConcurrentConsumers { get; }

        int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount);
    }
}
