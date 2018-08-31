using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConnection
    {
        private readonly ILogger _logger;
        public IConnection Connection { get; set; }

        public RabbitMQConnection(IConnection connection, ILogger logger)
        {
            _logger = logger;
            Connection = connection;
        }

        public int LocalPort => Connection.LocalPort;

        public int RemotePort => Connection.RemotePort;

        public void Dispose()
        {
            Connection.Dispose();
        }

        public void Abort()
        {
            Connection.Abort();
        }

        public void Abort(ushort reasonCode, string reasonText)
        {
            Connection.Abort(reasonCode, reasonText);
        }

        public void Abort(int timeout)
        {
            Connection.Abort(timeout);
        }

        public void Abort(ushort reasonCode, string reasonText, int timeout)
        {
            Connection.Abort(reasonCode, reasonText, timeout);
        }

        public void Close()
        {
            Connection.Close();
        }

        public void Close(ushort reasonCode, string reasonText)
        {
            Connection.Close(reasonCode, reasonText);
        }

        public void Close(int timeout)
        {
            Connection.Close(timeout);
        }

        public void Close(ushort reasonCode, string reasonText, int timeout)
        {
            Connection.Close(reasonCode, reasonText, timeout);
        }

        public RabbitMQModel CreateModel(bool subscribeEvents)
        {
            var model = Connection.CreateModel();
            var rabbitMQModel = new RabbitMQModel(_logger, model, RequeueModelAction, DiscardModelAction, subscribeEvents);
            return rabbitMQModel;
        }

        private void DiscardModelAction()
        {
            _logger?.LogInformation("DiscardModelAction triggered");
        }

        private void RequeueModelAction(RabbitMQModel mqModel)
        {
            _logger?.LogInformation("RequeueModelAction triggered");
        }

        public void HandleConnectionBlocked(string reason)
        {
            Connection.HandleConnectionBlocked(reason);
        }

        public void HandleConnectionUnblocked()
        {
            Connection.HandleConnectionUnblocked();
        }

        public bool AutoClose
        {
            get => Connection.AutoClose;
            set => Connection.AutoClose = value;
        }

        public ushort ChannelMax => Connection.ChannelMax;

        public IDictionary<string, object> ClientProperties => Connection.ClientProperties;

        public ShutdownEventArgs CloseReason => Connection.CloseReason;

        public AmqpTcpEndpoint Endpoint => Connection.Endpoint;

        public uint FrameMax => Connection.FrameMax;

        public ushort Heartbeat => Connection.Heartbeat;

        public bool IsOpen => Connection.IsOpen;

        public AmqpTcpEndpoint[] KnownHosts => Connection.KnownHosts;

        public IProtocol Protocol => Connection.Protocol;

        public IDictionary<string, object> ServerProperties => Connection.ServerProperties;

        public IList<ShutdownReportEntry> ShutdownReport => Connection.ShutdownReport;

        public string ClientProvidedName => Connection.ClientProvidedName;

        public ConsumerWorkService ConsumerWorkService => Connection.ConsumerWorkService;

        public event EventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add => Connection.CallbackException += value;
            remove => Connection.CallbackException -= value;
        }

        public event EventHandler<EventArgs> RecoverySucceeded
        {
            add => Connection.RecoverySucceeded += value;
            remove => Connection.RecoverySucceeded -= value;
        }

        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError
        {
            add => Connection.ConnectionRecoveryError += value;
            remove => Connection.ConnectionRecoveryError -= value;
        }

        public event EventHandler<ConnectionBlockedEventArgs> ConnectionBlocked
        {
            add => Connection.ConnectionBlocked += value;
            remove => Connection.ConnectionBlocked -= value;
        }

        public event EventHandler<ShutdownEventArgs> ConnectionShutdown
        {
            add => Connection.ConnectionShutdown += value;
            remove => Connection.ConnectionShutdown -= value;
        }

        public event EventHandler<EventArgs> ConnectionUnblocked
        {
            add => Connection.ConnectionUnblocked += value;
            remove => Connection.ConnectionUnblocked -= value;
        }
    }
}