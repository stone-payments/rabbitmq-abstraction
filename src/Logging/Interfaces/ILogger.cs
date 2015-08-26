namespace Vtex.RabbitMQ.Logging.Interfaces
{
    public interface ILogger
    {
        void Debug(string context, string content, params string[] tags);

        void Error(string context, string content, params string[] tags);

        void Fatal(string context, string content, params string[] tags);

        void Trace(string context, string content, params string[] tags);

        void Warn(string context, string content, params string[] tags);

        void Audit(string context, string content, params string[] tags);
    }
}
