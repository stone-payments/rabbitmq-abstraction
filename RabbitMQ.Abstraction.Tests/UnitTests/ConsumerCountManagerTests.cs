using Shouldly;
using Xunit;

namespace RabbitMQ.Abstraction.Tests.UnitTests
{
    public class ConsumerCountManagerTests
    {
        [Fact]
        public void Given_a_queue_with_more_than_consumer_quantity_messages_consumer_manager_should_create_max_of_consumers_with_success()
        {
            // Arrrange
            var consumerManager = new ConsumerCountManager(1, 10, 1, 500);
            var totalOfScallingConsummers = 9;

            var queueInfo = new QueueInfo
            {
                ConsumerCount = 1,
                MessageCount = 10000,
                QueueName = "Any Queue"
            };

            var exepectedConsumers = consumerManager.GetScalingAmount(queueInfo, 1);

            totalOfScallingConsummers.ShouldBe(exepectedConsumers);
        }
    }
}
