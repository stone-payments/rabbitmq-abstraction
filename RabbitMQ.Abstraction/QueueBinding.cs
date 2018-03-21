namespace RabbitMQ.Abstraction
{
    public class QueueBinding
    {
        public Queue Queue { get; private set; }

        public string Route { get; private set; }

        public QueueBinding(string queue, string route)
        {
            Queue = new Queue(queue);
            Route = route;
        }

        public QueueBinding(Queue queue, string route)
        {
            Queue = queue;
            Route = route;
        }
    }
}