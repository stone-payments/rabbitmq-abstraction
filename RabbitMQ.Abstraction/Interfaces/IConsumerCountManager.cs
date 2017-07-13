using System;

namespace RabbitMQ.Abstraction.Interfaces
{
    public interface IConsumerCountManager
    {
        TimeSpan AutoscaleFrequency { get; set; }

        int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount);
    }
}
