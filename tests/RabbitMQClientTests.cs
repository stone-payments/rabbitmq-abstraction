using System.Linq;
using System.Text;
using Newtonsoft.Json;
using NSubstitute;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;
using Vtex.RabbitMQ.Messaging;
using Vtex.RabbitMQ.Messaging.Interfaces;
using Vtex.RabbitMQ.Serialization.Interfaces;

namespace Vtex.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQClientTests
    {
        [Test]
        public void Should_Publish()
        {
            //Arrange
            var model = Substitute.For<IModel>();
            model.CreateBasicProperties().Returns(new BasicProperties());
            var connection = Substitute.For<IConnection>();
            connection.CreateModel().Returns(model);
            var connectionPool = Substitute.For<IRabbitMQConnectionPool>();
            connectionPool.GetConnection().Returns(connection);
            var queueClient = new RabbitMQClient(connectionPool);
            var payload = GetPayload("someMessage");

            //Act
            queueClient.Publish("someExchange", "someRoute", "someMessage");

            //Assert
            model.Received().BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                Arg.Is<byte[]>(byteData => payload.SequenceEqual(byteData)));
        }

        [Test]
        public void Should_Publish_Batch()
        {
            //Arrange
            var model = Substitute.For<IModel>();
            model.CreateBasicProperties().Returns(new BasicProperties());
            var connection = Substitute.For<IConnection>();
            connection.CreateModel().Returns(model);
            var connectionPool = Substitute.For<IRabbitMQConnectionPool>();
            connectionPool.GetConnection().Returns(connection);
            var queueClient = new RabbitMQClient(connectionPool);
            var payload1 = GetPayload("someMessage1");
            var payload2 = GetPayload("someMessage2");
            var payload3 = GetPayload("someMessage3");

            //Act
            queueClient.BatchPublish("someExchange", "someRoute", new[] {"someMessage1", "someMessage2", "someMessage3"});

            //Assert
            model.Received().BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                Arg.Is<byte[]>(byteData => payload1.SequenceEqual(byteData)));
            model.Received().BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                Arg.Is<byte[]>(byteData => payload2.SequenceEqual(byteData)));
            model.Received().BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                Arg.Is<byte[]>(byteData => payload3.SequenceEqual(byteData)));
        }

        [Test]
        public void Should_Publish_Persistent()
        {
            //Arrange
            var model = Substitute.For<IModel>();
            model.CreateBasicProperties().Returns(new BasicProperties());
            var connection = Substitute.For<IConnection>();
            connection.CreateModel().Returns(model);
            var connectionPool = Substitute.For<IRabbitMQConnectionPool>();
            connectionPool.GetConnection().Returns(connection);
            var queueClient = new RabbitMQClient(connectionPool);

            //Act
            queueClient.Publish("someExchange", "someRoute", "someMessage");

            //Assert
            model.Received().BasicPublish("someExchange", "someRoute", 
                Arg.Is<IBasicProperties>(prop => prop.DeliveryMode == 2), Arg.Any<byte[]>());
        }

        [Test]
        public void Should_Publish_Batch_Transactional()
        {
            //Arrange
            var model = Substitute.For<IModel>();
            model.CreateBasicProperties().Returns(new BasicProperties());
            var connection = Substitute.For<IConnection>();
            connection.CreateModel().Returns(model);
            var connectionPool = Substitute.For<IRabbitMQConnectionPool>();
            connectionPool.GetConnection().Returns(connection);
            var queueClient = new RabbitMQClient(connectionPool);
            var payload1 = GetPayload("someMessage1");
            var payload2 = GetPayload("someMessage2");
            var payload3 = GetPayload("someMessage3");

            //Act
            queueClient.BatchPublishTransactional("someExchange", "someRoute", new[] { "someMessage1", "someMessage2",
                "someMessage3" });

            //Assert
            Received.InOrder(() =>
            {
                model.TxSelect();
                model.BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                    Arg.Is<byte[]>(byteData => payload1.SequenceEqual(byteData)));
                model.BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                    Arg.Is<byte[]>(byteData => payload2.SequenceEqual(byteData)));
                model.BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                    Arg.Is<byte[]>(byteData => payload3.SequenceEqual(byteData)));
                model.TxCommit();
            });
        }

        [Test]
        public void Should_Use_ISerializer()
        {
            //Arrange
            var model = Substitute.For<IModel>();
            model.CreateBasicProperties().Returns(new BasicProperties());
            var connection = Substitute.For<IConnection>();
            connection.CreateModel().Returns(model);
            var connectionPool = Substitute.For<IRabbitMQConnectionPool>();
            connectionPool.GetConnection().Returns(connection);
            var serializer = Substitute.For<ISerializer>();
            serializer.Serialize(Arg.Any<object>()).Returns("someSerializedMessage");
            var queueClient = new RabbitMQClient(connectionPool, serializer);
            var payload = Encoding.UTF8.GetBytes("someSerializedMessage");

            //Act
            queueClient.Publish("someExchange", "someRoute", "someMessage");

            //Assert
            serializer.Received().Serialize("someMessage");
            model.Received().BasicPublish("someExchange", "someRoute", Arg.Any<IBasicProperties>(), 
                Arg.Is<byte[]>(byteData => payload.SequenceEqual(byteData)));
        }

        private static byte[] GetPayload<T>(T message)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
        }
    }
}
