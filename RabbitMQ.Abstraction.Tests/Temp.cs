using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.ProcessingWorkers;

namespace RabbitMQ.Abstraction.Tests
{
    class Temp
    {
        public async Task TempFunc()
        {
            using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
            {
                //Processing Function
                Task ProcessingFunc(string message, CancellationToken cancellationToken)
                {
                    Console.WriteLine(message);

                    return Task.FromResult(0);
                }

                //Processing Timeout
                var processingTimeout = TimeSpan.FromSeconds(10);

                //Custom IConsumerCountManager

                var advancedAsyncMessageProcessingWorker = await AdvancedAsyncProcessingWorker<string>.CreateAndStartAsync(queueClient,
                    "someQueue", ProcessingFunc, processingTimeout, CancellationToken.None);

                advancedAsyncMessageProcessingWorker.Stop();
            }
        }
    }

    class RandomConsumerCountManager : IConsumerCountManager
    {
        public TimeSpan AutoscaleFrequency { get; set; }

        public RandomConsumerCountManager()
        {
            AutoscaleFrequency = TimeSpan.FromSeconds(5);
        }

        public int GetScalingAmount(QueueInfo queueInfo, int consumersRunningCount)
        {
            return new Random().Next(1, 100);
        }
    }
}
