using System;
using System.Collections.Generic;
using System.Linq;
using RabbitMQ.Client;
using Vtex.RabbitMQ.Messaging.Interfaces;

namespace Vtex.RabbitMQ.Messaging
{
    public class RabbitMQConnectionPool : IRabbitMQConnectionPool
    {
        private readonly ConnectionFactory _connectionFactory;

        private readonly List<IConnection> _connections;

        private readonly uint _poolSize;

        public RabbitMQConnectionPool(ConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;

            _connections = new List<IConnection>();

            if (_poolSize == 0)
            {
                _poolSize = 1;
            }

            EnsurePoolSize();
        }

        public IConnection GetConnection()
        {
            EnsurePoolSize();

            IConnection elegibleConnection;

            lock(_connections)
            {
                elegibleConnection = _connections.FirstOrDefault(c => c.IsOpen);
            }

            if(elegibleConnection == null)
            {
                elegibleConnection = GetConnection();
            }

            var connection = EnsureConnectionOpen(elegibleConnection);

            return connection;
        }

        private IConnection EnsureConnectionOpen(IConnection connection)
        {
            IConnection ensuredConnection;

            try
            {
                var model = connection.CreateModel();

                model.Dispose();

                ensuredConnection = connection;
            }
            catch (Exception)
            {
                //TODO: Review this lock
                lock (_connections)
                {
                    _connections.Remove(connection);
                }

                ensuredConnection = GetConnection();
            }
            
            return ensuredConnection;
        }

        private void EnsurePoolSize()
        {
            lock(_connections)
            {
                _connections.RemoveAll(c => c.IsOpen == false);

                var newConnectionsNeeded = Convert.ToInt32(_poolSize - _connections.Count);

                for (var i = 0; i < newConnectionsNeeded; i++)
                {
                    _connections.Add(_connectionFactory.CreateConnection());
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

                _connections.RemoveAll(c => c.IsOpen == false);
            }
        }
    }
}
