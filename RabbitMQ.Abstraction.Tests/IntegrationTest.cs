using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.ProcessingWorkers;
using RabbitMQ.Client;
using Shouldly;
using Xunit;

namespace RabbitMQ.Abstraction.Tests
{
    public class IntegrationTest
    {
        [Fact]
        public async Task CreatePublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName, autoDelete:true);

                queueClient.Publish("", queueName, "TestValue123");

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

        [Fact]
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

                queueClient.QueueDelete(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Fact]
        public async Task CreateBatchPublishAndBatchConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 100000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    batch => BatchDoSomething(batch, receivedMessages), 200, CancellationToken.None, new ConsumerCountManager(1, 1));

                const int timeLimit = 30000;

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

        [Fact]
        public async Task CreateBatchPublishAndBatchConsumePartialNack()
        {
            var connectionFactory = CreateConnectionFactory();

            const int messageAmount = 200000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";
                
                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

                var receivedMessages = new ConcurrentBag<string>();

                var worker = await SimpleProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    batch => BatchDoSomethingOrFail(batch, receivedMessages), 200, CancellationToken.None, new ConsumerCountManager(1, 1));

                const int timeLimit = 60000;

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

        [Fact]
        public async Task AdvancedCreatePublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName, autoDelete: true);

                queueClient.Publish("", queueName, "TestValue123");

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

        [Fact]
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

                queueClient.QueueDelete(queueName);

                receivedMessages.Count.ShouldBe(messages.Count);
                receivedMessages.ShouldBeSubsetOf(messages);
            }
        }

        [Fact]
        public async Task ConsumerScaling()
        {
            var connectionFactory = CreateConnectionFactory();

            int messageAmount = 300000;

            var messages = GenerateMessages(messageAmount);

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName);

                queueClient.BatchPublish("", queueName, messages);

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
                        queueClient.BatchPublish("", queueName, messages);
                        messageAmount += 1000;
                    }
                }

                worker.Stop();

                queueClient.QueueDelete(queueName);
            }
        }

        [Fact]
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

        [Fact]
        public async Task CreateVirtualHost()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var virtualHostName = $"IntegratedTestVhost_{Guid.NewGuid()}";
                
                var success = await queueClient.VirtualHostDeclare(virtualHostName);

                success.ShouldBeTrue();
            }
        }

        [Fact]
        public async Task CreateVirtualHostAndGrantPermission()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var virtualHostName = $"IntegratedTestVhost_{Guid.NewGuid()}";

                var success = await queueClient.VirtualHostDeclare(virtualHostName);

                success.ShouldBeTrue();

                success = await queueClient.GrantPermissions(virtualHostName, "guest",
                    new VirtualHostUserPermission {Configure = ".*", Read = ".*", Write = ".*"});

                success.ShouldBeTrue();
            }
        }

        [Fact]
        public async Task AdvancedAsyncCreatePublishAndConsume()
        {
            var connectionFactory = CreateConnectionFactory();

            using (var queueClient = new RabbitMQClient(connectionFactory))
            {
                var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

                queueClient.EnsureQueueExists(queueName, autoDelete: true);

                queueClient.Publish("", queueName, "TestValue123");

                var receivedMessage = "";

                var worker = await AdvancedAsyncProcessingWorker<string>.CreateAndStartAsync(queueClient, queueName,
                    DoNothingAsync, TimeSpan.FromDays(1), CancellationToken.None);

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

        [Fact]
        public async Task AutoAck()
        {
            var queueConsumerMock = new Mock<RabbitMQConsumer<string>>();

            var consumerWorkerMock = new Mock<RabbitMQConsumerWorker<string>>();

            queueConsumerMock.Setup(c => c.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            queueConsumerMock.Protected().Setup<IQueueConsumerWorker>("CreateNewConsumerWorker")
                .Returns(consumerWorkerMock.Object);

            consumerWorkerMock.Setup(w => w.DoConsumeAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            var queueClientMock = new Mock<IQueueClient>();
            queueClientMock.Setup(m => m.GetConsumer(It.IsAny<string>(), It.IsAny<IConsumerCountManager>(),
                    It.IsAny<IMessageProcessingWorker<string>>(), It.IsAny<IMessageRejectionHandler>()))
                .Returns(queueConsumerMock.Object);
            
            var queueName = $"IntegratedTestQueue_{Guid.NewGuid()}";

            var worker = await AdvancedAsyncProcessingWorker<string>.CreateAndStartAsync(queueClientMock.Object, queueName,
                DoNothingAsync, TimeSpan.FromDays(1), CancellationToken.None);

            //queueConsumerMock

            worker.Stop();
        }

        private static ConnectionFactory CreateConnectionFactory()
        {
            return new ConnectionFactory
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/"
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

        private static Task DoNothingAsync(string message, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
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
