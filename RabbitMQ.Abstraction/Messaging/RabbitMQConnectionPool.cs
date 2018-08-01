using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConnectionPool : IDisposable
    {
        public readonly ConnectionFactory ConnectionFactory;

        private ConcurrentQueue<IRabbitMQConnection> _connections;
        private readonly object _connectionsLock = new object();

        private readonly uint _poolSize;

        private readonly uint _modelPoolSize;

        private readonly ILogger _logger;

        private static readonly string ClientIdentifier =
            $"{Assembly.GetEntryAssembly().GetName().Name}(v{Assembly.GetEntryAssembly().GetName().Version})@{Environment.MachineName}";

        public RabbitMQConnectionPool(ConnectionFactory connectionFactory, uint poolSize = 1, uint modelPoolSize = 1, ILogger logger = null)
        {
            ConnectionFactory = connectionFactory;

            _connections = new ConcurrentQueue<IRabbitMQConnection>();

            _poolSize = poolSize == 0 ? 1 : poolSize;
            _modelPoolSize = modelPoolSize == 0 ? 1 : modelPoolSize;

            _logger = logger;

            EnsurePoolSize();
        }

        public async Task<IRabbitMQConnection> GetConnectionAsync()
        {
            IRabbitMQConnection elegibleConnection;
            bool success;

            do
            {
                lock (_connectionsLock)
                {
                    success = _connections.TryDequeue(out elegibleConnection);

                    if (success)
                    {
                        if (elegibleConnection.IsOpen)
                        {
                            _connections.Enqueue(elegibleConnection);
                        }
                    }
                }

                if (success && !elegibleConnection.IsOpen)
                {
                    elegibleConnection.Dispose();

                    success = false;
                }

                if (!success)
                {
                    EnsurePoolSize();

                    await Task.Delay(TimeSpan.FromMilliseconds(50));
                }
            } while (!success);

            return elegibleConnection;
        }

        public IRabbitMQConnection CreateConnection(uint modelPoolSize = 1, string customIdentifier = null)
        {
            try
            {
                var identifier = ClientIdentifier;

                if (!string.IsNullOrWhiteSpace(customIdentifier))
                {
                    identifier += $"/{customIdentifier}";
                }

                var connection = ConnectionFactory.CreateConnection(identifier);

                return new RabbitMQConnection(connection, modelPoolSize);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "Unable to create connection");
                throw;
            }
        }

        private void EnsurePoolSize()
        {
            lock(_connectionsLock)
            {
                var openConnections = new ConcurrentQueue<IRabbitMQConnection>();

                foreach (var rabbitMQConnection in _connections)
                {
                    if (rabbitMQConnection.IsOpen)
                    {
                        openConnections.Enqueue(rabbitMQConnection);
                    }
                    else
                    {
                        rabbitMQConnection.Dispose();
                    }
                }

                _connections = openConnections;

                var newConnectionsNeeded = _poolSize - _connections.Count;

                Parallel.For(0, newConnectionsNeeded, i =>
                {
                    var success = false;

                    do
                    {
                        try
                        {
                            _connections.Enqueue(
                                new RabbitMQConnection(ConnectionFactory.CreateConnection(ClientIdentifier),
                                    _modelPoolSize));

                            success = true;
                        }
                        catch (Exception e)
                        {
                            _logger?.LogError(e, "Unable to create pool connection");

                            Task.Delay(TimeSpan.FromMilliseconds(50));
                        }
                    } while (!success);
                });
            }
        }

        public void Dispose()
        {
            lock(_connectionsLock)
            {
                foreach (var connection in _connections)
                {
                    connection.Dispose();
                }

                _connections = null;
            }
        }
    }
}
