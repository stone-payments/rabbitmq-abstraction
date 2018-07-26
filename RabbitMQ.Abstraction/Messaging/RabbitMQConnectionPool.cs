using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConnectionPool : IDisposable
    {
        public readonly ConnectionFactory ConnectionFactory;

        private ConcurrentQueue<IRabbitMQConnection> _connections;

        private readonly uint _poolSize;

        private readonly uint _modelPoolSize;

        public RabbitMQConnectionPool(ConnectionFactory connectionFactory, uint poolSize = 1, uint modelPoolSize = 1)
        {
            ConnectionFactory = connectionFactory;

            _connections = new ConcurrentQueue<IRabbitMQConnection>();

            _poolSize = poolSize == 0 ? 1 : poolSize;
            _modelPoolSize = modelPoolSize == 0 ? 1 : modelPoolSize;

            EnsurePoolSize();
        }

        public async Task<IRabbitMQConnection> GetConnectionAsync()
        {
            IRabbitMQConnection elegibleConnection;

            while(!_connections.TryDequeue(out elegibleConnection))
            {
                EnsurePoolSize();

                await Task.Delay(TimeSpan.FromMilliseconds(50));
            }

            _connections.Enqueue(elegibleConnection);

            return elegibleConnection;
        }

        public IRabbitMQConnection CreateConnection(uint modelPoolSize = 1)
        {
            return new RabbitMQConnection(ConnectionFactory.CreateConnection(), modelPoolSize);
        }

        private void EnsurePoolSize()
        {
            lock(_connections)
            {
                _connections = new ConcurrentQueue<IRabbitMQConnection>(_connections.Where(c => c.IsOpen));

                var newConnectionsNeeded = _poolSize - _connections.Count;

                for (var i = 0; i < newConnectionsNeeded; i++)
                {
                    _connections.Enqueue(new RabbitMQConnection(ConnectionFactory.CreateConnection(), _modelPoolSize));
                }
            }
        }

        public void Dispose()
        {
            lock(_connections)
            {
                foreach (var connection in _connections.Where(c => c.IsOpen))
                {
                    connection.Close();
                }

                _connections = null;
            }
        }
    }
}
