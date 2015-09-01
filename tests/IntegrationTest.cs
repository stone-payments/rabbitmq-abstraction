using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

                var worker = new SimpleMessageProcessingWorker<string>(queueClient, queueName,
                    message => DoSomething(message, out receivedMessage));

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

                var worker = new SimpleMessageProcessingWorker<string>(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages));

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

                var worker = new AdvancedMessageProcessingWorker<string>(queueClient, queueName,
                    message => DoSomething(message, out receivedMessage));

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

                var worker = new AdvancedMessageProcessingWorker<string>(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages));

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

        private static ConnectionFactory CreateConnectionFactory()
        {
            return new ConnectionFactory
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "geust",
                Password = "guest",
                VirtualHost = "virtualHost"
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
