using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.ProcessingWorkers;
using RabbitMQ.Client;
using Shouldly;

namespace RabbitMQ.Abstraction.Tests
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

                await queueClient.EnsureQueueExistsAsync(queueName, autoDelete:true);

                await queueClient.PublishAsync("", queueName, "TestValue123");

                var receivedMessage = "";

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
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
        public async Task CreatePriorityPublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                await queueClient.EnsureQueueExistsAsync(queueName, autoDelete: true, arguments: new Dictionary<string, object>{{"x-max-priority", 1}});

                await queueClient.PublishAsync("", queueName, "TestValue123", 0);
                await queueClient.PublishAsync("", queueName, "TestValue456", 1);
                await queueClient.PublishAsync("", queueName, "TestValue789", 1);
                await queueClient.PublishAsync("", queueName, "TestValue321", 0);

                var receivedMessages = new List<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message =>
                    {
                        DoSomething(message, out var receivedMessage);

                        receivedMessages.Add(receivedMessage);
                    }, CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessages.Count < 4 && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                receivedMessages.Count.ShouldBe(4);
                receivedMessages[0].ShouldBe("TestValue456");
                receivedMessages[1].ShouldBe("TestValue789");
                receivedMessages[2].ShouldBe("TestValue123");
                receivedMessages[3].ShouldBe("TestValue321");
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

                await queueClient.EnsureQueueExistsAsync(queueName);

                await queueClient.BatchPublishAsync("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                await queueClient.QueueDeleteAsync(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Test]
        public async Task CreateBatchPublishAndBatchConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 100000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                await queueClient.EnsureQueueExistsAsync(queueName);

                await queueClient.BatchPublishAsync("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    batch => BatchDoSomething(batch, receivedMessages), 200, CancellationToken.None, new ConsumerCountManager(1, 1));

                const int timeLimit = 40000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                await queueClient.QueueDeleteAsync(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Test]
        public async Task CreateBatchPublishAndBatchConsumePartialNack()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 200000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";
                
                await queueClient.EnsureQueueExistsAsync(queueName);

                await queueClient.BatchPublishAsync("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    batch => BatchDoSomethingOrFail(batch, receivedMessages), 200, CancellationToken.None, new ConsumerCountManager(1, 1));

                const int timeLimit = 90000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                await queueClient.QueueDeleteAsync(queueName);

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

                await queueClient.EnsureQueueExistsAsync(queueName, autoDelete: true);

                await queueClient.PublishAsync("", queueName, "TestValue123");

                var receivedMessage = "";

                var worker = await AdvancedProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
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

                await queueClient.EnsureQueueExistsAsync(queueName);

                await queueClient.BatchPublishAsync("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await AdvancedProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    message => BatchDoSomething(message, receivedMessages), CancellationToken.None);

                const int timeLimit = 10000;

                var elapsedTime = 0;

                while (receivedMessages.Count < messageAmount && elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;
                }

                worker.Stop();

                await queueClient.QueueDeleteAsync(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Test]
        public async Task ConsumerScaling()
        {
            var connectionFactory = CreateConnectionFactory();

            int messageAmount = 300000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                await queueClient.EnsureQueueExistsAsync(queueName);

                await queueClient.BatchPublishAsync("", queueName, messages);

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                            async message => await Task.Delay(20), CancellationToken.None, new ConsumerCountManager(4, 50, 1000, 2000));

                const int timeLimit = 60000;

                var elapsedTime = 0;

                while (elapsedTime < timeLimit)
                {
                    await Task.Delay(100);
                    elapsedTime += 100;

                    if (elapsedTime%1000 == 0 && messageAmount < 70000)
                    {
                        messages = GenerateMessages(10000);
                        await queueClient.BatchPublishAsync("", queueName, messages);
                        messageAmount += 1000;
                    }
                }

                worker.Stop();

                await queueClient.QueueDeleteAsync(queueName);
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
                    await queueClient.QueueDeclareAsync(queueName);
                    await queueClient.ExchangeDeclareAsync("delayedTargetExchange");
                    await queueClient.QueueBindAsync(queueName, "delayedTargetExchange", "delayedTargetRoutingKey");

                    await queueClient.DelayedPublishAsync("delayedTargetExchange", "delayedTargetRoutingKey", "delayedMessage",
                        TimeSpan.FromSeconds(5));

                    (await queueClient.GetMessageCountAsync(queueName)).ShouldBe<uint>(0);

                    await Task.Delay(TimeSpan.FromSeconds(5));

                    (await queueClient.GetMessageCountAsync(queueName)).ShouldBe<uint>(1);
                }
                finally
                {
                    await queueClient.QueueDeleteAsync(queueName);
                }
            }
        }

        [Test]
        public async Task CreateShovelWithTargetQueue()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var response = await queueClient.ShovelDeclareAsync("testing", "testShovelWithTargetQueue",
                    new ShovelConfiguration(new ShovelConfigurationContent("amqp://guest:guest@localhost/testing",
                        "testSourceQueue", "amqp://guest:guest@localhost/testing", "testTargetQueue")));

                if (!response)
                {
                    throw new Exception("Error creating shovel");
                }
            }
        }

        [Test]
        public async Task CreateShovelWithTargetExchangeAndRoutingKey()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var response = await queueClient.ShovelDeclareAsync("testing", "testShovelWithTargetExchangeAndRoutingKey",
                    new ShovelConfiguration(new ShovelConfigurationContent("amqp://guest:guest@localhost/testing",
                        "testSourceQueue", "amqp://guest:guest@localhost/testing", "testTargetExchange", "testTargetRoutingKey")));

                if (!response)
                {
                    throw new Exception("Error creating shovel");
                }
            }
        }

        [Test]
        public async Task CreatePolicy()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var response = await queueClient.PolicyDeclareAsync("testing", "testPolicy",
                    new VirtualHostPolicy("(.log)$", new Dictionary<string, object> {{"message-ttl", 1000}}, 0, PolicyScope.Queues));

                if (!response)
                {
                    throw new Exception("Error creating policy");
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

        private static void BatchDoSomething(IEnumerable<string> messages, ConcurrentBag<string> receivedMessages)
        {
            foreach (var message in messages)
            {
                receivedMessages.Add(message);
            }
        }

        private static void BatchDoSomethingOrFail(IEnumerable<string> messages, ConcurrentBag<string> receivedMessages)
        {
            if (new Random().Next(0, 10) == 0)
            {
                throw new Exception();
            }

            foreach (var message in messages)
            {
                receivedMessages.Add(message);
            }
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
