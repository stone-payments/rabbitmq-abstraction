namespace Vtex.RabbitMQ
{
    public class QueueInfo
    {
        public uint ConsumerCount { get; set; }

        public uint MessageCount { get; set; }

        public string QueueName { get; set; }
    }
}
