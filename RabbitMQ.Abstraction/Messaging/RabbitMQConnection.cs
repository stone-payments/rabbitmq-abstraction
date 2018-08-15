using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConnection : IRabbitMQConnection
    {
        private readonly IConnection _connection;

        private readonly RabbitMQModelPool _rabbitMQModelPool;

        public RabbitMQConnection(IConnection connection, ILogger logger, uint modelPoolSize = 1)
        {
            _connection = connection;

            try
            {
                _rabbitMQModelPool = new RabbitMQModelPool(_connection.CreateModel, logger, modelPoolSize);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public Task<IModel> GetModelAsync()
        {
            return _rabbitMQModelPool.GetModelAsync();
        }

        public int LocalPort => _connection.LocalPort;

        public int RemotePort => _connection.RemotePort;

        public void Dispose()
        {
            _rabbitMQModelPool.Dispose();

            _connection.Dispose();
        }

        public void Abort()
        {
            _connection.Abort();
        }

        public void Abort(ushort reasonCode, string reasonText)
        {
            _connection.Abort(reasonCode, reasonText);
        }

        public void Abort(int timeout)
        {
            _connection.Abort(timeout);
        }

        public void Abort(ushort reasonCode, string reasonText, int timeout)
        {
            _connection.Abort(reasonCode, reasonText, timeout);
        }

        public void Close()
        {
            _connection.Close();
        }

        public void Close(ushort reasonCode, string reasonText)
        {
            _connection.Close(reasonCode, reasonText);
        }

        public void Close(int timeout)
        {
            _connection.Close(timeout);
        }

        public void Close(ushort reasonCode, string reasonText, int timeout)
        {
            _connection.Close(reasonCode, reasonText, timeout);
        }

        public IModel CreateModel()
        {
            return _connection.CreateModel();
        }

        public void HandleConnectionBlocked(string reason)
        {
            _connection.HandleConnectionBlocked(reason);
        }

        public void HandleConnectionUnblocked()
        {
            _connection.HandleConnectionUnblocked();
        }

        public bool AutoClose
        {
            get => _connection.AutoClose;
            set => _connection.AutoClose = value;
        }

        public ushort ChannelMax => _connection.ChannelMax;

        public IDictionary<string, object> ClientProperties => _connection.ClientProperties;

        public ShutdownEventArgs CloseReason => _connection.CloseReason;

        public AmqpTcpEndpoint Endpoint => _connection.Endpoint;

        public uint FrameMax => _connection.FrameMax;

        public ushort Heartbeat => _connection.Heartbeat;

        public bool IsOpen => _connection.IsOpen;

        public AmqpTcpEndpoint[] KnownHosts => _connection.KnownHosts;

        public IProtocol Protocol => _connection.Protocol;

        public IDictionary<string, object> ServerProperties => _connection.ServerProperties;

        public IList<ShutdownReportEntry> ShutdownReport => _connection.ShutdownReport;

        public string ClientProvidedName => _connection.ClientProvidedName;

        public ConsumerWorkService ConsumerWorkService => _connection.ConsumerWorkService;

        public event EventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add => _connection.CallbackException += value;
            remove => _connection.CallbackException -= value;
        }

        public event EventHandler<EventArgs> RecoverySucceeded
        {
            add => _connection.RecoverySucceeded += value;
            remove => _connection.RecoverySucceeded -= value;
        }

        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError
        {
            add => _connection.ConnectionRecoveryError += value;
            remove => _connection.ConnectionRecoveryError -= value;
        }

        public event EventHandler<ConnectionBlockedEventArgs> ConnectionBlocked
        {
            add => _connection.ConnectionBlocked += value;
            remove => _connection.ConnectionBlocked -= value;
        }

        public event EventHandler<ShutdownEventArgs> ConnectionShutdown
        {
            add => _connection.ConnectionShutdown += value;
            remove => _connection.ConnectionShutdown -= value;
        }

        public event EventHandler<EventArgs> ConnectionUnblocked
        {
            add => _connection.ConnectionUnblocked += value;
            remove => _connection.ConnectionUnblocked -= value;
        }
    }
}
