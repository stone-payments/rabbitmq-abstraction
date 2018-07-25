using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.IO;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQPersistentConnection : IRabbitMQPersistentConnection
    {
        public ConnectionFactory ConnectionFactory { get; set; }

        private readonly object _locker = new object();

        private bool _initialized;

        public IConnection connection { get; set; }

        private readonly ILogger _logger;

        public RabbitMQPersistentConnection(ConnectionFactory connectionFactory, ILogger logger)
        {
            this.ConnectionFactory = connectionFactory;
            _logger = logger;

            Initialize();
        }

        public void Initialize()
        {
            lock (_locker)
            {
                if (_initialized)
                {
                    throw new Exception("This PersistentConnection has already been initialized.");
                }
                _initialized = true;
                TryToConnect();
            }
        }

        public IModel CreateModel()
        {
            if (!IsConnected)
            {
                throw new Exception("PersistentConnection: Attempt to create a channel while being disconnected.");
            }

            return connection.CreateModel();
        }

        public bool IsConnected => connection != null && connection.IsOpen && !_disposed;

        private void TryToConnect()
        {
            if (_disposed) return;

            bool succeeded = false;

            try
            {
                connection = ConnectionFactory.CreateConnection(); // A possible dispose race condition exists, whereby the Dispose() method may run while this loop is waiting on connectionFactory.CreateConnection() returning a connection.  In that case, a connection could be created and assigned to the connection variable, without it ever being later disposed, leading to app hang on shutdown.  The following if clause guards against this condition and ensures such connections are always disposed.

                if (_disposed)
                {
                    connection.Dispose();
                }

                succeeded = true;
            }
            catch (SocketException socketException)
            {
                LogException(socketException);
            }
            catch (BrokerUnreachableException brokerUnreachableException)
            {
                LogException(brokerUnreachableException);
            }

            if (succeeded)
            {
                connection.ConnectionShutdown += OnConnectionShutdown;
            }
            else
            {
                if (!_disposed)
                {
                    TryToConnect();
                }
            }
        }

        private void LogException(Exception exception)
        {
            var exceptionMessage = exception.Message;
            // if there is an inner exception, surface its message since it has more detailed information on why connection failed
            if (exception.InnerException != null)
            {
                exceptionMessage = $"{exceptionMessage} ({exception.InnerException.Message})";
            }

            _logger?.LogError(exception, exceptionMessage);
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            if (_disposed) return;

            TryToConnect();
        }

        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (connection != null)
            {
                try
                {
                    connection.Dispose();
                }
                catch (IOException exception)
                {
                }
            }
        }
    }
}