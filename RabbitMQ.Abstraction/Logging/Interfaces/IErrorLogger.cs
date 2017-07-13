namespace RabbitMQ.Abstraction.Logging.Interfaces
{
    public interface IErrorLogger
    {
        void LogError(string context, string content, params string[] tags);
    }
}
