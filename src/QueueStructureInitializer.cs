using System.Collections.Generic;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ
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

        public void QueueInit(IEnumerable<QueueBinding> queueBindings, bool enableDeadLettering = true)
        {
            _queueClient.ExchangeDeclare(_exchangeName);

            foreach (var queueBinding in queueBindings)
            {
                //Error queue
                var errorQueueName = "error." + queueBinding.Queue;
                var errorRouteName = _exchangeName + "." + errorQueueName;
                QueueDeclareAndBind(errorQueueName, errorRouteName);

                //Process queue
                var processQueueName = "process." + queueBinding.Queue;
                var processRouteName = queueBinding.Route;

                if (enableDeadLettering)
                {
                    QueueDeclareAndBind(processQueueName, processRouteName, errorRouteName);
                }
                else
                {
                    QueueDeclareAndBind(processQueueName, processRouteName);
                }

                //Log queue
                var logQueueName = "log." + queueBinding.Queue;
                QueueDeclareAndBind(logQueueName, processRouteName);
            }
        }

        protected void QueueDeclareAndBind(string queueName, string routeName, string deadLetterRouteName = null)
        {
            var queueArguments = new Dictionary<string, object>();

            if (!string.IsNullOrWhiteSpace(deadLetterRouteName))
            {
                queueArguments.Add("x-dead-letter-exchange", _exchangeName);
                queueArguments.Add("x-dead-letter-routing-key", deadLetterRouteName);
            }

            _queueClient.EnsureQueueExists(queueName, arguments: queueArguments);
            _queueClient.QueueBind(queueName, _exchangeName, routeName);
        }
    }
}
