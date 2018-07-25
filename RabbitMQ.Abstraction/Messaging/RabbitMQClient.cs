using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using JsonSerializer = RabbitMQ.Abstraction.Serialization.JsonSerializer;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQClient : IQueueClient
    {
        private readonly ISerializer _serializer;

        private readonly ILogger _logger;

        private readonly IRabbitMQPersistentConnection _persistentConnection;

        private readonly HttpClient _httpClient;

        private readonly Regex _connectionStringPattern =
            new Regex(@"^(?<user>.+):(?<password>.+)@(?<host>.+):(?<port>[0-9]{1,5})/(?<vhost>.+)$");

        /// <summary>
        ///
        /// </summary>
        /// <param name="connectionString">Format {user}:{password}@{host}:{port}/{virtualHost}</param>
        /// <param name="serializer"></param>
        /// <param name="logger"></param>
        public RabbitMQClient(string connectionString, ISerializer serializer = null, ILogger logger = null)
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
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = new TimeSpan(0, 0, 0, 10)
            };

            _persistentConnection = new RabbitMQPersistentConnection(connectionFactory, logger);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(match.Groups["user"].Value, match.Groups["password"].Value, match.Groups["host"].Value, int.Parse(match.Groups["port"].Value));
        }

        public RabbitMQClient(string hostName, int port, string userName, string password, string virtualHost,
            ISerializer serializer = null, ILogger logger = null)
        {
            var connectionFactory = new ConnectionFactory
            {
                HostName = hostName,
                Port = port,
                UserName = userName,
                Password = password,
                VirtualHost = virtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = new TimeSpan(0, 0, 0, 10)
            };

            _persistentConnection = new RabbitMQPersistentConnection(connectionFactory, logger);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(userName, password, hostName, port);
        }

        public RabbitMQClient(ConnectionFactory connectionFactory, ISerializer serializer = null, ILogger logger = null)
        {
            _persistentConnection = new RabbitMQPersistentConnection(connectionFactory, logger);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(connectionFactory.UserName, connectionFactory.Password, connectionFactory.HostName, connectionFactory.Port);
        }

        public RabbitMQClient(IRabbitMQPersistentConnection persistentConnection, ISerializer serializer = null, ILogger logger = null)
        {
            _persistentConnection = persistentConnection;
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(
                _persistentConnection.ConnectionFactory.UserName,
                _persistentConnection.ConnectionFactory.Password,
                _persistentConnection.ConnectionFactory.HostName,
                _persistentConnection.ConnectionFactory.Port
                );
        }

        private static HttpClient GetHttpClient(string username, string password, string host, int port)
        {
            return new HttpClient
            {
                BaseAddress = new Uri($"http://{host}:1{port}/api/"),
                DefaultRequestHeaders =
                {
                    Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes($"{username}:{password}")))
                }
            };
        }

        public Task PublishAsync<T>(string exchangeName, string routingKey, T content, byte? priority = null)
        {
            var serializedContent = _serializer.Serialize(content);
            using (var model = _persistentConnection.CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;

                if (priority != null)
                {
                    props.Priority = priority.Value;
                }

                var payload = Encoding.UTF8.GetBytes(serializedContent);
                model.BasicPublish(exchangeName, routingKey, props, payload);
            }

            return Task.CompletedTask;
        }

        public Task BatchPublishAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;

                if (priority != null)
                {
                    props.Priority = priority.Value;
                }

                foreach (var content in contentList)
                {
                    var serializedContent = _serializer.Serialize(content);

                    var payload = Encoding.UTF8.GetBytes(serializedContent);
                    model.BasicPublish(exchangeName, routingKey, props, payload);
                }
            }

            return Task.CompletedTask;
        }

        public Task BatchPublishTransactionalAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                try
                {
                    model.TxSelect();

                    var props = model.CreateBasicProperties();
                    props.DeliveryMode = 2;

                    if (priority != null)
                    {
                        props.Priority = priority.Value;
                    }

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

            return Task.CompletedTask;
        }

        public async Task DelayedPublishAsync<T>(string exchangeName, string routingKey, T content, TimeSpan delay, byte? priority = null)
        {
            var serializedContent = _serializer.Serialize(content);
            using (var model = _persistentConnection.CreateModel())
            {
                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;

                if (priority != null)
                {
                    props.Priority = priority.Value;
                }

                props.Expiration = delay.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
                var payload = Encoding.UTF8.GetBytes(serializedContent);

                var queueName = $"delayed.{routingKey}@{exchangeName}.{Guid.NewGuid()}";

                var queueArguments = new Dictionary<string, object>
                {
                    {"x-dead-letter-exchange", exchangeName},
                    {"x-dead-letter-routing-key", routingKey},
                    {"x-expires", (long) delay.Add(TimeSpan.FromSeconds(1)).TotalMilliseconds},
                };

                if (priority != null)
                {
                    queueArguments.Add("x-max-priority", priority);
                }

                await QueueDeclareAsync(queueName, arguments: queueArguments);

                model.BasicPublish("", queueName, props, payload);
            }
        }

        public Task QueueDeclareAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                model.QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);
            }

            return Task.CompletedTask;
        }

        public Task QueueDeclarePassiveAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                model.QueueDeclarePassive(queueName);
            }

            return Task.CompletedTask;
        }

        public Task<uint> QueueDeleteAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                return Task.FromResult(model.QueueDelete(queueName));
            }
        }

        public Task QueueBindAsync(string queueName, string exchangeName, string routingKey)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                model.QueueBind(queueName, exchangeName, routingKey);
            }

            return Task.CompletedTask;
        }

        public Task ExchangeDeclareAsync(string exchangeName, bool passive = false)
        {
            using (var model = _persistentConnection.CreateModel())
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

            return Task.CompletedTask;
        }

        public Task<bool> QueueExistsAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                try
                {
                    model.QueueDeclarePassive(queueName);
                }
                catch (Exception)
                {
                    return Task.FromResult(false);
                }

                return Task.FromResult(true);
            }
        }

        public async Task EnsureQueueExistsAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null)
        {
            if (!await QueueExistsAsync(queueName).ConfigureAwait(false))
            {
                await QueueDeclareAsync(queueName, durable, exclusive, autoDelete, arguments).ConfigureAwait(false);
            }
        }

        public Task<uint> QueuePurgeAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                var returnValue = model.QueuePurge(queueName);
                return Task.FromResult(returnValue);
            }
        }

        public Task<uint> GetMessageCountAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                var queueDeclareOk = model.QueueDeclarePassive(queueName);

                return Task.FromResult(queueDeclareOk.MessageCount);
            }
        }

        public Task<uint> GetConsumerCountAsync(string queueName)
        {
            using (var model = _persistentConnection.CreateModel())
            {
                var queueDeclareOk = model.QueueDeclarePassive(queueName);

                return Task.FromResult(queueDeclareOk.ConsumerCount);
            }
        }

        public async Task<bool> VirtualHostDeclareAsync(string virtualHostName)
        {
            var response = await _httpClient.PutAsync($"vhosts/{virtualHostName}", null).ConfigureAwait(false);

            return response.IsSuccessStatusCode;
        }

        public async Task<bool> GrantPermissionsAsync(string virtualHostName, string userName, VirtualHostUserPermission permissions)
        {
            var response = await _httpClient.PutAsync($"permissions/{virtualHostName}/{userName}",
                new StringContent(JsonConvert.SerializeObject(permissions), Encoding.UTF8, "application/json")).ConfigureAwait(false);

            return response.IsSuccessStatusCode;
        }

        public async Task<bool> PolicyDeclareAsync(string virtualHostName, string policyName, VirtualHostPolicy policy)
        {
            var response = await _httpClient.PutAsync($"policies/{virtualHostName}/{policyName}",
                new StringContent(JsonConvert.SerializeObject(policy), Encoding.UTF8, "application/json")).ConfigureAwait(false);

            return response.IsSuccessStatusCode;
        }

        public async Task<bool> ShovelDeclareAsync(string virtualHostName, string shovelName,
            ShovelConfiguration shovelConfiguration)
        {
            var response = await _httpClient.PutAsync($"/api/parameters/shovel/{virtualHostName}/{shovelName}",
                    new StringContent(
                        JsonConvert.SerializeObject(shovelConfiguration,
                            new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore }), Encoding.UTF8,
                        "application/json"))
                .ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }

        public IQueueConsumer CreateConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class
        {
            return new RabbitMQConsumer<T>(
                persistentConnection: _persistentConnection,
                queueName: queueName,
                serializer: _serializer,
                logger: _logger,
                messageProcessingWorker: messageProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler);
        }

        public IQueueConsumer CreateBatchConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IBatchProcessingWorker<T> batchProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class
        {
            return new RabbitMQBatchConsumer<T>(
                persistentConnection: _persistentConnection,
                queueName: queueName,
                serializer: _serializer,
                logger: _logger,
                batchProcessingWorker: batchProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler);
        }

        public void Dispose()
        {
            _persistentConnection?.Dispose();
        }
    }
}