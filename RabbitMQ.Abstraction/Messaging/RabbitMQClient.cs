using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Abstraction.Interfaces;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Abstraction.Serialization.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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

        private readonly IConnection _connectionPublisher;

        private readonly IConnection _connectionConsumer;

        private readonly IModel _modelPublisher;

        private readonly ConnectionFactory _connectionFactory;

        private readonly HttpClient _httpClient;

        private static readonly object ModelPublisherLock = new object();

        private readonly Regex _connectionStringPattern =
            new Regex(@"^(?<user>.+):(?<password>.+)@(?<host>.+):(?<port>[0-9]{1,5})/(?<vhost>.+)$");

        ///  <summary>
        /// 
        ///  </summary>
        ///  <param name="connectionString">Format {user}:{password}@{host}:{port}/{virtualHost}</param>
        ///  <param name="serializer"></param>
        ///  <param name="logger"></param>
        /// <param name="connectionPoolSize"></param>
        /// <param name="modelPoolSize"></param>
        /// <param name="heartbeat"></param>
        public RabbitMQClient(string connectionString, ISerializer serializer = null, ILogger logger = null, uint connectionPoolSize = 1, uint modelPoolSize = 1, ushort heartbeat = 60)
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

            _connectionPublisher = _connectionFactory.CreateConnection();
            _connectionConsumer = _connectionFactory.CreateConnection();
            _modelPublisher = _connectionPublisher.CreateModel();
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(match.Groups["user"].Value, match.Groups["password"].Value, match.Groups["host"].Value, int.Parse(match.Groups["port"].Value));

            SubscribeConnectionEvents();
        }

        public RabbitMQClient(string hostName, int port, string userName, string password, string virtualHost,
            ISerializer serializer = null, ILogger logger = null, uint connectionPoolSize = 1, uint modelPoolSize = 1, ushort heartbeat = 60)
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

            _connectionPublisher = _connectionFactory.CreateConnection();
            _connectionConsumer = _connectionFactory.CreateConnection();
            _modelPublisher = _connectionPublisher.CreateModel();
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(userName, password, hostName, port);

            SubscribeConnectionEvents();
        }

        public RabbitMQClient(ConnectionFactory connectionFactory, ISerializer serializer = null, ILogger logger = null)
        {
            _connectionPublisher = _connectionFactory.CreateConnection();
            _connectionConsumer = _connectionFactory.CreateConnection();
            _modelPublisher = _connectionPublisher.CreateModel();
            _serializer = serializer ?? new JsonSerializer();
            _logger = logger;

            _httpClient = GetHttpClient(connectionFactory.UserName, connectionFactory.Password, connectionFactory.HostName, connectionFactory.Port);

            SubscribeConnectionEvents();
        }

        private void SubscribeConnectionEvents()
        {
            _connectionPublisher.ConnectionShutdown += Connection_ConnectionShutdown;
            _connectionPublisher.ConnectionRecoveryError += Connection_ConnectionRecoveryError;
            _connectionPublisher.CallbackException += Connection_CallbackException;
            _connectionPublisher.ConnectionBlocked += Connection_ConnectionBlocked;
            _connectionPublisher.ConnectionUnblocked += Connection_ConnectionUnblocked;
            _connectionPublisher.RecoverySucceeded += Connection_RecoverySucceeded;

            _connectionConsumer.ConnectionShutdown += Connection_ConnectionShutdown;
            _connectionConsumer.ConnectionRecoveryError += Connection_ConnectionRecoveryError;
            _connectionConsumer.CallbackException += Connection_CallbackException;
            _connectionConsumer.ConnectionBlocked += Connection_ConnectionBlocked;
            _connectionConsumer.ConnectionUnblocked += Connection_ConnectionUnblocked;
            _connectionConsumer.RecoverySucceeded += Connection_RecoverySucceeded;

            _modelPublisher.CallbackException += ModelPublisher_CallbackException;
            _modelPublisher.BasicRecoverOk += ModelPublisher_BasicRecoverOk;
            _modelPublisher.ModelShutdown += ModelPublisher_ModelShutdown;
        }

        private void UnsubscribeConnectionEvents()
        {
            _connectionPublisher.ConnectionShutdown -= Connection_ConnectionShutdown;
            _connectionPublisher.ConnectionRecoveryError -= Connection_ConnectionRecoveryError;
            _connectionPublisher.CallbackException -= Connection_CallbackException;
            _connectionPublisher.ConnectionBlocked -= Connection_ConnectionBlocked;
            _connectionPublisher.ConnectionUnblocked -= Connection_ConnectionUnblocked;
            _connectionPublisher.RecoverySucceeded -= Connection_RecoverySucceeded;

            _connectionConsumer.ConnectionShutdown -= Connection_ConnectionShutdown;
            _connectionConsumer.ConnectionRecoveryError -= Connection_ConnectionRecoveryError;
            _connectionConsumer.CallbackException -= Connection_CallbackException;
            _connectionConsumer.ConnectionBlocked -= Connection_ConnectionBlocked;
            _connectionConsumer.ConnectionUnblocked -= Connection_ConnectionUnblocked;
            _connectionConsumer.RecoverySucceeded -= Connection_RecoverySucceeded;

            _modelPublisher.CallbackException -= ModelPublisher_CallbackException;
            _modelPublisher.BasicRecoverOk -= ModelPublisher_BasicRecoverOk;
            _modelPublisher.ModelShutdown -= ModelPublisher_ModelShutdown;
        }

        private void ModelPublisher_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQModel Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText} ");
        }

        private void ModelPublisher_BasicRecoverOk(object sender, EventArgs e)
        {
            _logger?.LogInformation("RabbitMQModel Basic Recover Ok.");
        }

        private void ModelPublisher_CallbackException(object sender, CallbackExceptionEventArgs e)
        {
            _logger?.LogError(e.Exception, $"RabbitMQModel Shutdown. Message: {e.Exception.Message}{Environment.NewLine}StackTrace: {e.Exception.StackTrace} ");
        }

        private void Connection_RecoverySucceeded(object sender, EventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection Recovery Succeeded");
        }

        private void Connection_ConnectionUnblocked(object sender, EventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection Unblocked");
        }

        private void Connection_ConnectionBlocked(object sender, Client.Events.ConnectionBlockedEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection Blocked");
        }

        private void Connection_CallbackException(object sender, Client.Events.CallbackExceptionEventArgs e)
        {
            _logger?.LogError(e.Exception, $"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection CallbackException. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void Connection_ConnectionRecoveryError(object sender, Client.Events.ConnectionRecoveryErrorEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection Recovery Error. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void Connection_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{(sender == _connectionPublisher ? "publisher connection" : "consumer connection")}] Connection Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText}");
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
                using (var model = _connectionPublisher.CreateModel())
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
            });
        }

        public async Task DelayedPublishAsync<T>(string exchangeName, string routingKey, T content, TimeSpan delay, byte? priority = null)
        {
            var serializedContent = _serializer.Serialize(content);
            using (var model = _connectionPublisher.CreateModel())
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

                await QueueDeclareAsync(queueName, arguments: queueArguments).ConfigureAwait(false);

                model.BasicPublish("", queueName, props, payload);
            }
        }

        public Task QueueDeclareAsync(string queueName, bool durable = true, bool exclusive = false, bool autoDelete = false,
            IDictionary<string, object> arguments = null)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    model.QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);
                }
            });
        }

        public Task QueueDeclarePassiveAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    model.QueueDeclarePassive(queueName);
                }
            });
        }

        public Task<uint> QueueDeleteAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    return model.QueueDelete(queueName);
                }
            });
        }

        public Task QueueBindAsync(string queueName, string exchangeName, string routingKey)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    model.QueueBind(queueName, exchangeName, routingKey);
                }
            });
        }

        public Task ExchangeDeclareAsync(string exchangeName, bool passive = false)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
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
            });
        }

        public Task<bool> QueueExistsAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
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
                using (var model = _connectionPublisher.CreateModel())
                {
                    var returnValue = model.QueuePurge(queueName);
                    return returnValue;
                }
            });
        }

        public Task<uint> GetMessageCountAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    var queueDeclareOk = model.QueueDeclarePassive(queueName);

                    return queueDeclareOk.MessageCount;
                }
            });
        }

        public Task<uint> GetConsumerCountAsync(string queueName)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var model = _connectionPublisher.CreateModel())
                {
                    var queueDeclareOk = model.QueueDeclarePassive(queueName);

                    return queueDeclareOk.ConsumerCount;
                }
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
            return new RabbitMQConsumer<T>(
                queueClient: this,
                //connection: _connectionFactory.CreateConnection(),
                //connectionPublisher: _connectionFactory.CreateConnection(),
                connectionConsumer: _connectionConsumer,
                connectionPublisher: _connectionPublisher,
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
            return new RabbitMQBatchConsumer<T>(
                //connection: _connectionFactory.CreateConnection(),
                //connectionPublisher: _connectionFactory.CreateConnection(),
                connection: _connectionConsumer,
                connectionPublisher: _connectionPublisher,
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
            UnsubscribeConnectionEvents();
            _connectionConsumer?.Dispose();
            _connectionPublisher?.Dispose();
        }
    }
}
