using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using Shouldly;
using Vtex.RabbitMQ.Messaging;
using Vtex.RabbitMQ.ProcessingWorkers;

namespace Vtex.RabbitMQ.Tests
{
    [TestFixture]
    public class IntegrationTest
    {
        [Test]
        public async Task CreatePublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName, autoDelete:true);

                queueClient.Publish("", queueName, "TestValue123");

                var receivedMessage = "";

                var worker = await SimpleMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => DoSomething(message, out receivedMessage), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessage == "" && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                receivedMessage.ShouldBe("TestValue123");
            }
        }

        [Test]
        public async Task CreateBatchPublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 100;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                queueClient.QueueDelete(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Test]
        public async Task AdvancedCreatePublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName, autoDelete: true);

                queueClient.Publish("", queueName, "TestValue123");

                var receivedMessage = "";

                var worker = await AdvancedMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => DoSomething(message, out receivedMessage), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessage == "" && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                receivedMessage.ShouldBe("TestValue123");
            }
        }

        [Test]
        public async Task AdvancedCreateBatchPublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 100;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await AdvancedMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                queueClient.QueueDelete(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Test]
        public async Task ConsumerScaling()
        {
            var connectionFactory = CreateConnectionFactory();

            int messageAmount = 30000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

                var worker = await SimpleMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                            async message => await Task.Delay(20), CancellationToken.None, new ConsumerCountManager(4, 50, 1000, 2000));

                const int timeLimit = 1200000;

                var elapsedTime = 0;

                while (elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;

                    if (elapsedTime%1000 == 0 && messageAmount < 70000)
                    {
                        messages = GenerateMessages(1000);
                        queueClient.BatchPublish("", queueName, messages);
                        messageAmount += 1000;
                    }
                }

                worker.Stop();

                queueClient.QueueDelete(queueName);
            }
        }

        [Test]
        public async Task DelayedPublish()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                try
                {
                    queueClient.QueueDeclare(queueName);
                    queueClient.ExchangeDeclare("delayedTargetExchange");
                    queueClient.QueueBind(queueName, "delayedTargetExchange", "delayedTargetRoutingKey");

                    queueClient.DelayedPublish("delayedTargetExchange", "delayedTargetRoutingKey", "delayedMessage",
                        TimeSpan.FromSeconds(5));

                    queueClient.GetMessageCount(queueName).ShouldBe<uint>(0);

                    await Task.Delay(TimeSpan.FromSeconds(5));

                    queueClient.GetMessageCount(queueName).ShouldBe<uint>(1);
                }
                finally
                {
                    queueClient.QueueDelete(queueName);
                }
            }
        }

        private static ConnectionFactory CreateConnectionFactory()
        {
            return new ConnectionFactory
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "testing"
            };
        }

        private static void DoSomething(string message, out string receivedMessage)
        {
            receivedMessage = message;
        }

        private static void BatchDoSomething(string message, ConcurrentBag<string> receivedMessages)
        {
            receivedMessages.Add(message);
        }

        private static List<string> GenerateMessages(int amount)
        {
            var list = new List<string>();

            for (var i = 0; i < amount; i++)
            {
                list.Add("message" + i);
            }

            return list;
        }
    }
}
