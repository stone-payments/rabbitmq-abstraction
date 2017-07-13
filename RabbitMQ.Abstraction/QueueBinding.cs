namespace RabbitMQ.Abstraction
{
    public class QueueBinding
    {
        public string Queue { get; private set; }

        public string Route { get; private set; }

        public QueueBinding(string queue, string route)
        {
            Queue = queue;
            Route = route;
        }
    }
}