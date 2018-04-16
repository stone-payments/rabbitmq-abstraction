using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.IO;
using System.Net.Sockets;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQPersistentConnection : IRabbitMQPersistentConnection
    {
        private readonly IConnectionFactory _connectionFactory;

        private readonly object locker = new object();

        private bool initialized;

        public IConnection connection { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        public string HostName { get; set; }

        public int Port { get; set; }

        public RabbitMQPersistentConnection(IConnectionFactory connectionFactory)
        {
            this._connectionFactory = connectionFactory;
            UserName = "guest";
            Password = "guest";
            HostName = "localhost";
            Port = 5672;

            Initialize();
        }

        public void Initialize()
        {
            lock (locker)
            {
                if (initialized)
                {
                    throw new Exception("This PersistentConnection has already been initialized.");
                }
                initialized = true;
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

        public bool IsConnected => connection != null && connection.IsOpen && !disposed;

        private void TryToConnect()
        {
            if (disposed) return;

            bool Succeeded = false;

            try
            {
                connection = _connectionFactory.CreateConnection(); // A possible dispose race condition exists, whereby the Dispose() method may run while this loop is waiting on connectionFactory.CreateConnection() returning a connection.  In that case, a connection could be created and assigned to the connection variable, without it ever being later disposed, leading to app hang on shutdown.  The following if clause guards against this condition and ensures such connections are always disposed.

                if (disposed)
                {
                    connection.Dispose();
                }

                Succeeded = true;
            }
            catch (SocketException socketException)
            {
                LogException(socketException);
            }
            catch (BrokerUnreachableException brokerUnreachableException)
            {
                LogException(brokerUnreachableException);
            }

            if (Succeeded)
            {
                connection.ConnectionShutdown += OnConnectionShutdown;
                connection.ConnectionBlocked += OnConnectionBlocked;
                connection.ConnectionUnblocked += OnConnectionUnblocked;
                OnConnected();
            }
            else
            {
                if (!disposed)
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
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            if (disposed) return;
            OnDisconnected();

            TryToConnect();
        }

        private void OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
        }

        private void OnConnectionUnblocked(object sender, EventArgs e)
        {
        }

        public void OnConnected()
        {
        }

        public void OnDisconnected()
        {
        }

        private bool disposed;

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
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