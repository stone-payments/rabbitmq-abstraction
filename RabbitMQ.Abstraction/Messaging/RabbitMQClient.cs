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

        private readonly ConnectionFactory _connectionFactory;

        private readonly HttpClient _httpClient;

        private static readonly object ModelPublisherLock = new object();

        private readonly RabbitMQConnectionManager _rabbitMQConnectionManager;

        private readonly RabbitMQModel _modelPublisher;

        public bool ConnectionPerQueue { get; set; }

        private readonly Regex _connectionStringPattern =
            new Regex(@"^(?<user>.+):(?<password>.+)@(?<host>.+):(?<port>[0-9]{1,5})/(?<vhost>.+)$");

        public RabbitMQClient(bool connectionPerQueue)
        {
            ConnectionPerQueue = connectionPerQueue;
        }

        ///   <summary>
        ///
        ///   </summary>
        /// <param name="connectionString">Format {user}:{password}@{host}:{port}/{virtualHost}</param>
        /// <param name="connectionPerQueue"></param>
        /// <param name="serializer"></param>
        /// <param name="logger"></param>
        /// <param name="heartbeat"></param>
        public RabbitMQClient(string connectionString, ISerializer serializer = null, ILogger logger = null, ushort heartbeat = 60, bool connectionPerQueue = true)
            : this(connectionPerQueue)
        {
            var match = _connectionStringPattern.Match(connectionString);
            if (!match.Success)
                throw new ArgumentException("Expected format: {user}:{password}@{host}:{port}/{virtualHost}", nameof(connectionString));

            _connectionFactory = new ConnectionFactory
            {
                HostName = match.Groups["host"].Value,
                Port = int.Parse(match.Groups["port"].Value),
                UserName = match.Groups["user"].Value,
                Password = match.Groups["password"].Value,
                VirtualHost = match.Groups["vhost"].Value,
                AutomaticRecoveryEnabled = true,
                RequestedHeartbeat = heartbeat,
            };

            _rabbitMQConnectionManager = new RabbitMQConnectionManager(_connectionFactory, logger);
            _rabbitMQConnectionManager.CreateConnection("publisher");
            _modelPublisher = _rabbitMQConnectionManager.ConnectionByName("publisher").CreateModel(false);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(match.Groups["user"].Value, match.Groups["password"].Value, match.Groups["host"].Value, int.Parse(match.Groups["port"].Value));
        }

        public RabbitMQClient(string hostName, int port, string userName, string password, string virtualHost,
            ISerializer serializer = null, ILogger logger = null, ushort heartbeat = 60, bool connectionPerQueue = true)
            : this(connectionPerQueue)
        {
            _connectionFactory = new ConnectionFactory
            {
                HostName = hostName,
                Port = port,
                UserName = userName,
                Password = password,
                VirtualHost = virtualHost,
                AutomaticRecoveryEnabled = true,
                RequestedHeartbeat = heartbeat,
            };

            _rabbitMQConnectionManager = new RabbitMQConnectionManager(_connectionFactory, logger);
            _rabbitMQConnectionManager.CreateConnection("publisher");
            _modelPublisher = _rabbitMQConnectionManager.ConnectionByName("publisher").CreateModel(false);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(userName, password, hostName, port);
        }

        public RabbitMQClient(ConnectionFactory connectionFactory, ISerializer serializer = null, ILogger logger = null, bool connectionPerQueue = true)
            : this(connectionPerQueue)
        {
            _rabbitMQConnectionManager = new RabbitMQConnectionManager(connectionFactory, logger);
            _rabbitMQConnectionManager.CreateConnection("publisher");
            _modelPublisher = _rabbitMQConnectionManager.ConnectionByName("publisher").CreateModel(false);
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(connectionFactory.UserName, connectionFactory.Password, connectionFactory.HostName, connectionFactory.Port);
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
            return Task.Factory.StartNew(() =>
            {
                lock (ModelPublisherLock)
                {
                    var serializedContent = _serializer.Serialize(content);

                    var props = _modelPublisher.CreateBasicProperties();
                    props.DeliveryMode = 2;

                    if (priority != null)
                    {
                        props.Priority = priority.Value;
                    }

                    var payload = Encoding.UTF8.GetBytes(serializedContent);
                    _modelPublisher.BasicPublish(exchangeName, routingKey, props, payload);
                }
            });
        }

        public Task PublishAsync<T>(IModel model, string exchangeName, string routingKey, T content, byte? priority = null)
        {
            return Task.Factory.StartNew(() =>
            {
                var serializedContent = _serializer.Serialize(content);

                var props = model.CreateBasicProperties();
                props.DeliveryMode = 2;

                if (priority != null)
                {
                    props.Priority = priority.Value;
                }

                var payload = Encoding.UTF8.GetBytes(serializedContent);
                model.BasicPublish(exchangeName, routingKey, props, payload);
            });
        }

        public Task BatchPublishAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null)
        {
            return Task.Factory.StartNew(() =>
            {
                lock (ModelPublisherLock)
                {
                    var props = _modelPublisher.CreateBasicProperties();
                    props.DeliveryMode = 2;

                    if (priority != null)
                    {
                        props.Priority = priority.Value;
                    }

                    foreach (var content in contentList)
                    {
                        var serializedContent = _serializer.Serialize(content);

                        var payload = Encoding.UTF8.GetBytes(serializedContent);
                        _modelPublisher.BasicPublish(exchangeName, routingKey, props, payload);
                    }
                }
            });
        }

        public Task BatchPublishAsync<T>(IModel model, string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null)
        {
            return Task.Factory.StartNew(() =>
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
            });
        }

        public Task BatchPublishTransactionalAsync<T>(string exchangeName, string routingKey, IEnumerable<T> contentList, byte? priority = null)
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    _modelPublisher.TxSelect();

                    var props = _modelPublisher.CreateBasicProperties();
                    props.DeliveryMode = 2;

                    if (priority != null)
                    {
                        props.Priority = priority.Value;
                    }

                    foreach (var content in contentList)
                    {
                        var serializedContent = _serializer.Serialize(content);

                        var payload = Encoding.UTF8.GetBytes(serializedContent);
                        _modelPublisher.BasicPublish(exchangeName, routingKey, props, payload);
                    }

                    _modelPublisher.TxCommit();
                }
                catch (Exception)
                {
                    if (_modelPublisher.IsOpen)
                    {
                        _modelPublisher.TxRollback();
                    }

                    throw;
                }
            });
        }

        public async Task DelayedPublishAsync<T>(string exchangeName, string routingKey, T content, TimeSpan delay, byte? priority = null)
        {
            var serializedContent = _serializer.Serialize(content);

            var props = _modelPublisher.CreateBasicProperties();
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

            await QueueDeclareAsync(queueName, arguments: queueArguments).ConfigureAwait(false);

            _modelPublisher.BasicPublish("", queueName, props, payload);
        }

        public Task QueueDeclareAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null)
        {
            return Task.Factory.StartNew(() =>
            {
                _modelPublisher.QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);
            });
        }

        public Task QueueDeclarePassiveAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                _modelPublisher.QueueDeclarePassive(queueName);
            });
        }

        public Task<uint> QueueDeleteAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                return _modelPublisher.QueueDelete(queueName);
            });
        }

        public Task QueueBindAsync(string queueName, string exchangeName, string routingKey)
        {
            return Task.Factory.StartNew(() =>
            {
                _modelPublisher.QueueBind(queueName, exchangeName, routingKey);
            });
        }

        public Task ExchangeDeclareAsync(string exchangeName, bool passive = false)
        {
            return Task.Factory.StartNew(() =>
            {
                if (passive)
                {
                    _modelPublisher.ExchangeDeclarePassive(exchangeName);
                }
                else
                {
                    _modelPublisher.ExchangeDeclare(exchangeName, ExchangeType.Topic, true);
                }
            });
        }

        public Task<bool> QueueExistsAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    _modelPublisher.QueueDeclarePassive(queueName);
                }
                catch (Exception)
                {
                    return false;
                }

                return true;
            });
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
            return Task.Factory.StartNew(() =>
            {
                var returnValue = _modelPublisher.QueuePurge(queueName);
                return returnValue;
            });
        }

        public Task<uint> GetMessageCountAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                var queueDeclareOk = _modelPublisher.QueueDeclarePassive(queueName);

                return queueDeclareOk.MessageCount;
            });
        }

        public Task<uint> GetConsumerCountAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                var queueDeclareOk = _modelPublisher.QueueDeclarePassive(queueName);

                return queueDeclareOk.ConsumerCount;
            });
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
            IMessageProcessingWorker<T> messageProcessingWorker, IMessageRejectionHandler messageRejectionHandler,
            ushort prefetchCount = 1)
            where T : class
        {
            RabbitMQConnection connectionConsumer = ConnectionPerQueue
                ? _rabbitMQConnectionManager.CreateConnection($"consumer: {queueName}")
                : _rabbitMQConnectionManager.ConnectionByName("consumer") ?? _rabbitMQConnectionManager.CreateConnection("consumer");

            var connectionPublisher = _rabbitMQConnectionManager.ConnectionByName("publisher");

            return new RabbitMQConsumer<T>(
                queueClient: this,
                connectionConsumer: connectionConsumer,
                connectionPublisher: connectionPublisher,
                connectionFactory: _connectionFactory,
                queueName: queueName,
                serializer: _serializer,
                logger: _logger,
                messageProcessingWorker: messageProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler,
                prefetchCount: prefetchCount);
        }

        public IQueueConsumer CreateBatchConsumer<T>(string queueName, IConsumerCountManager consumerCountManager,
            IBatchProcessingWorker<T> batchProcessingWorker, IMessageRejectionHandler messageRejectionHandler)
            where T : class
        {
            RabbitMQConnection connectionConsumer = ConnectionPerQueue
                ? _rabbitMQConnectionManager.CreateConnection($"consumer: {queueName}")
                : _rabbitMQConnectionManager.ConnectionByName("consumer") ?? _rabbitMQConnectionManager.CreateConnection("consumer");

            var connectionPublisher = _rabbitMQConnectionManager.ConnectionByName("publisher");

            return new RabbitMQBatchConsumer<T>(
                connection: connectionConsumer,
                connectionPublisher: connectionPublisher,
                connectionFactory: _connectionFactory,
                queueName: queueName,
                serializer: _serializer,
                logger: _logger,
                batchProcessingWorker: batchProcessingWorker,
                consumerCountManager: consumerCountManager,
                messageRejectionHandler: messageRejectionHandler);
        }

        public void Dispose()
        {
            _rabbitMQConnectionManager.Dispose();
        }
    }
}