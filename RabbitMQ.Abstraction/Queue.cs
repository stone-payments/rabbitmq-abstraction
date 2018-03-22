namespace RabbitMQ.Abstraction
{
    public class Queue
    {
        public string Name { get; private set; }

        public sbyte? MaxPriority { get; private set; }

        public Queue(string name, sbyte? maxPriority = null)
        {
            Name = name;

            if (maxPriority != null)
            {
                MaxPriority = maxPriority.Value;
            }
        }

        public override string ToString()
        {
            return Name;
        }

        public static implicit operator string(Queue queue)
        {
            return queue.Name;
        }

        public static implicit operator Queue(string queue)
        {
            return new Queue(queue);
        }
    }
}