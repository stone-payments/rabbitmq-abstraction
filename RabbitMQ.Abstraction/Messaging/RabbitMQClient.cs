﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Logging.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using JsonSerializer = RabbitMQ.Abstraction.Serialization.JsonSerializer;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQClient : IQueueClient
    {
        private readonly ISerializer _serializer;

        private readonly IErrorLogger _errorLogger;

        private readonly RabbitMQConnectionPool _connectionPool;

        private readonly HttpClient _httpClient;

        private readonly Regex _connectionStringPattern =
            new Regex(@"^(?<user>.+):(?<password>.+)@(?<host>.+):(?<port>[0-9]{1,5})/(?<vhost>.+)$");

        /// <summary>
        ///
        /// </summary>
        /// <param name="connectionString">Format {user}:{password}@{host}:{port}/{virtualHost}</param>
        /// <param name="serializer"></param>
        /// <param name="errorLogger"></param>
        public RabbitMQClient(string connectionString, ISerializer serializer = null, IErrorLogger errorLogger = null)
        {
            var match = _connectionStringPattern.Match(connectionString);
            if (!match.Success)
                throw new ArgumentException("Expected format: {user}:{password}@{host}:{port}/{virtualHost}", nameof(connectionString));

            var connectionFactory = new ConnectionFactory
            {
                HostName = match.Groups["host"].Value,
                Port = int.Parse(match.Groups["port"].Value),
                UserName = match.Groups["user"].Value,
                Password = match.Groups["password"].Value,
                VirtualHost = match.Groups["vhost"].Value,
            };

            _connectionPool = new RabbitMQConnectionPool(connectionFactory);
            _serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri($"http://{match.Groups["user"].Value}:{match.Groups["password"].Value}@{match.Groups["host"].Value}:1{match.Groups["port"].Value}/api"),
            };
        }

        public RabbitMQClient(string hostName, int port, string userName, string password, string virtualHost,
            ISerializer serializer = null, IErrorLogger errorLogger = null)
        {
            var connectionFactory = new ConnectionFactory
            {
                HostName = hostName,
                Port = port,
                UserName = userName,
                Password = password,
                VirtualHost = virtualHost
            };

            _connectionPool = new RabbitMQConnectionPool(connectionFactory);
            _serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri($"http://{userName}:{password}@{hostName}:1{port}/api"),
            };
        }

        public RabbitMQClient(ConnectionFactory connectionFactory, ISerializer serializer = null, IErrorLogger errorLogger = null)
        {
            _connectionPool = new RabbitMQConnectionPool(connectionFactory);
            _serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri($"http://{connectionFactory.UserName}:{connectionFactory.Password}@{connectionFactory.HostName}:1{connectionFactory.Port}/api"),
            };
        }

        public RabbitMQClient(RabbitMQConnectionPool connectionPool, ISerializer serializer = null, IErrorLogger errorLogger = null)
        {
            _connectionPool = connectionPool;
            _serializer = serializer ?? new JsonSerializer();
            _errorLogger = errorLogger;

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri($"http://{connectionPool.ConnectionFactory.UserName}:{connectionPool.ConnectionFactory.Password}@{connectionPool.ConnectionFactory.HostName}:1{connectionPool.ConnectionFactory.Port}/api"),
            };
        }

        public void Publish<T>(string exchangeName, string routingKey, T content)
        {
            var serializedContent = _serializer.Serialize(content);
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;
                var payload = Encoding.UTF8.GetBytes(serializedContent);
                model.BasicPublish(exchangeName, routingKey, props, payload);
            }
        }

        public void BatchPublish<T>(string exchangeName, string routingKey, IEnumerable<T> contentList)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;

                foreach (var content in contentList)
                {
                    var serializedContent = _serializer.Serialize(content);

                    var payload = Encoding.UTF8.GetBytes(serializedContent);
                    model.BasicPublish(exchangeName, routingKey, props, payload);
                }
            }
        }

        public void BatchPublishTransactional<T>(string exchangeName, string routingKey, IEnumerable<T> contentList)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                try
                {
                    model.TxSelect();

                    var props = model.CreateBasicProperties();
                    props.DeliveryMode = 2;

                    foreach (var content in contentList)
                    {
                        var serializedContent = _serializer.Serialize(content);

                        var payload = Encoding.UTF8.GetBytes(serializedContent);
                        model.BasicPublish(exchangeName, routingKey, props, payload);
                    }

                    model.TxCommit();
                }
                catch (Exception)
                {
                    if (model.IsOpen)
                    {
                        model.TxRollback();
                    }
                    
                    throw;
                }
            }
        }

        public void DelayedPublish<T>(string exchangeName, string routingKey, T content, TimeSpan delay)
        {
            var serializedContent = _serializer.Serialize(content);
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;
                props.Expiration = delay.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
                var payload = Encoding.UTF8.GetBytes(serializedContent);

                var queueName = $"delayed.{routingKey}@{exchangeName}.{Guid.NewGuid()}";

                QueueDeclare(queueName,
                    arguments:
                        new Dictionary<string, object>
                        {
                            {"x-dead-letter-exchange", exchangeName},
                            {"x-dead-letter-routing-key", routingKey},
                            {"x-expires", (long)delay.Add(TimeSpan.FromSeconds(1)).TotalMilliseconds}
                        });

                model.BasicPublish("", queueName, props, payload);
            }
        }

        public void QueueDeclare(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false, 
            IDictionary<string, object> arguments = null)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                model.QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);
            }
        }

        public void QueueDeclarePassive(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                model.QueueDeclarePassive(queueName);
            }
        }

        public uint QueueDelete(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                return model.QueueDelete(queueName);
            }
        }

        public void QueueBind(string queueName, string exchangeName, string routingKey)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                model.QueueBind(queueName, exchangeName, routingKey);
            }
        }

        public void ExchangeDeclare(string exchangeName, bool passive = false)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                if (passive)
                {
                    model.ExchangeDeclarePassive(exchangeName);
                }
                else
                {
                    model.ExchangeDeclare(exchangeName, ExchangeType.Topic, true);
                }
            }
        }

        public bool QueueExists(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                try
                {
                    model.QueueDeclarePassive(queueName);
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            }
        }

        public void EnsureQueueExists(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false, 
            IDictionary<string, object> arguments = null)
        {
            if (!QueueExists(queueName))
            {
                QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);
            }
        }

        public uint QueuePurge(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var returnValue = model.QueuePurge(queueName);
                return returnValue;
            }
        }

        public uint GetMessageCount(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var queueDeclareOk = model.QueueDeclarePassive(queueName);

                return queueDeclareOk.MessageCount;
            }
        }

        public uint GetConsumerCount(string queueName)
        {
            using (var model = _connectionPool.GetConnection().CreateModel())
            {
                var queueDeclareOk = model.QueueDeclarePassive(queueName);

                return queueDeclareOk.ConsumerCount;
            }
        }

        public async Task<bool> VirtualHostDeclare(string virtualHostname)
        {
            var response = await _httpClient.PutAsync($"/vhosts/{virtualHostname}", new StringContent(""));

            return response.IsSuccessStatusCode;
        }

        public async Task<bool> GrantPermissions(string virtualHostName, string userName, VirtualHostUserPermission permissions)
        {
            var response = await _httpClient.PutAsync($"/permissions/{virtualHostName}/{userName}",
                new StringContent(JsonConvert.SerializeObject(permissions)));

            return response.IsSuccessStatusCode;
        }

        public async Task<bool> PolicyDeclare(string virtualHostName, string policyName, VirtualHostPolicy policy)
        {
            var response = await _httpClient.PutAsync($"/policies/{virtualHostName}/{policyName}",
                new StringContent(JsonConvert.SerializeObject(policy)));

            return response.IsSuccessStatusCode;
        }

        public IQueueConsumer GetConsumer<T>(string queueName, IConsumerCountManager consumerCountManager, 
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler) 
            where T : class
        {
            return new RabbitMQConsumer<T>(
                connectionPool: _connectionPool,
                queueName: queueName,
                serializer: _serializer,
                errorLogger: _errorLogger,
                messageProcessingWorker: messageProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler);
        }

        public IQueueConsumer GetBatchConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IBatchProcessingWorker<T> batchProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class
        {
            return new RabbitMQBatchConsumer<T>(
                connectionPool: _connectionPool,
                queueName: queueName,
                serializer: _serializer,
                errorLogger: _errorLogger,
                batchProcessingWorker: batchProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler);
        }

        public void Dispose()
        {
            _connectionPool?.Dispose();
        }
    }
}
