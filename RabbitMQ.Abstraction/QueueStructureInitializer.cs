using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace RabbitMQ.Abstraction
{
    public class QueueStructureInitializer
    {
        private readonly IQueueClient _queueClient;

        private readonly string _exchangeName;

        public QueueStructureInitializer(IQueueClient queueClient, string exchangeName)
        {
            _queueClient = queueClient;
            _exchangeName = exchangeName;
        }

        public async Task QueueInitAsync(IEnumerable<QueueBinding> queueBindings, bool enableDeadLettering = true)
        {
            await _queueClient.ExchangeDeclareAsync(_exchangeName).ConfigureAwait(false);

            await queueBindings.ForEachAsync(async queueBinding =>
            {
                //Error queue
                var errorQueueName = $"{queueBinding.Queue}.error";
                var errorRouteName = _exchangeName + "." + errorQueueName;
                await QueueDeclareAndBindAsync(errorQueueName, errorRouteName, maxPriority: queueBinding.Queue.MaxPriority).ConfigureAwait(false);

                //Process queue
                var processQueueName = $"{queueBinding.Queue}.processing";
                var processRouteName = queueBinding.Route;

                if (enableDeadLettering)
                {
                    await QueueDeclareAndBindAsync(processQueueName, processRouteName, errorRouteName, maxPriority: queueBinding.Queue.MaxPriority).ConfigureAwait(false);
                }
                else
                {
                    await QueueDeclareAndBindAsync(processQueueName, processRouteName, maxPriority: queueBinding.Queue.MaxPriority).ConfigureAwait(false);
                }

                //Log queue
                var logQueueName = $"{queueBinding.Queue}.log";
                await QueueDeclareAndBindAsync(logQueueName, processRouteName, lazy: true, maxPriority: queueBinding.Queue.MaxPriority).ConfigureAwait(false);
            });
        }

        protected async Task QueueDeclareAndBindAsync(string queueName, string routeName, string deadLetterRouteName = null, bool lazy = false, sbyte? maxPriority = null)
        {
            var queueArguments = new Dictionary<string, object>();

            if (!string.IsNullOrWhiteSpace(deadLetterRouteName))
            {
                queueArguments.Add("x-dead-letter-exchange", _exchangeName);
                queueArguments.Add("x-dead-letter-routing-key", deadLetterRouteName);
            }

            if (lazy)
            {
                queueArguments.Add("x-queue-mode", "lazy");
            }

            if (maxPriority != null)
            {
                queueArguments.Add("x-max-priority", maxPriority.Value);
            }

            await _queueClient.EnsureQueueExistsAsync(queueName, arguments: queueArguments).ConfigureAwait(false);
            await _queueClient.QueueBindAsync(queueName, _exchangeName, routeName).ConfigureAwait(false);
        }
    }
}
