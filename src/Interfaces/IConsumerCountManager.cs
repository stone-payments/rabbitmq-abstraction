using System;

namespace Vtex.RabbitMQ.Interfaces
{
    public interface IConsumerCountManager
    {
        TimeSpan AutoscaleFrequency { get; set; }

        int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount);
    }
}
