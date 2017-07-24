namespace RabbitMQ.Abstraction
{
    public class VirtualHostUserPermission
    {
        public string Configure { get; set; }

        public string Write { get; set; }

        public string Read { get; set; }
    }
}
