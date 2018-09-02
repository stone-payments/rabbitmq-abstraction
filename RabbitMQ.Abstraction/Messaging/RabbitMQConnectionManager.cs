using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.Impl;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQConnectionManager : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ILogger _logger;
        public IList<RabbitMQConnection> Connections { get; private set; }

        public RabbitMQConnectionManager(ConnectionFactory connectionFactory, ILogger logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
            Connections = new List<RabbitMQConnection>();
        }

        public RabbitMQConnection ConnectionByName(string connectionName)
        {
            return Connections.FirstOrDefault(connection => connection.ClientProvidedName == connectionName);
        }

        public RabbitMQConnection CreateConnection(string connectionName)
        {
            var connection = new RabbitMQConnection(_connectionFactory.CreateConnection(connectionName), _logger);
            Connections.Add(connection);
            SubscribeConnectionEvents(connection);
            return connection;
        }

        private void SubscribeConnectionEvents(RabbitMQConnection connection)
        {
            connection.ConnectionShutdown += Connection_ConnectionShutdown;
            connection.ConnectionRecoveryError += Connection_ConnectionRecoveryError;
            connection.CallbackException += Connection_CallbackException;
            connection.ConnectionBlocked += Connection_ConnectionBlocked;
            connection.ConnectionUnblocked += Connection_ConnectionUnblocked;
            connection.RecoverySucceeded += Connection_RecoverySucceeded;
        }

        private void UnsubscribeConnectionEvents(RabbitMQConnection connection)
        {
            connection.ConnectionShutdown -= Connection_ConnectionShutdown;
            connection.ConnectionRecoveryError -= Connection_ConnectionRecoveryError;
            connection.CallbackException -= Connection_CallbackException;
            connection.ConnectionBlocked -= Connection_ConnectionBlocked;
            connection.ConnectionUnblocked -= Connection_ConnectionUnblocked;
            connection.RecoverySucceeded -= Connection_RecoverySucceeded;
        }

        private string GetConnectionName(object connection)
        {
            if (connection is Connection)
            {
                return ((Connection)connection).ClientProvidedName;
            }

            return string.Empty;
        }

        private void Connection_RecoverySucceeded(object sender, EventArgs e)
        {
            _logger.LogInformation($"RabbitMQAbstraction[{GetConnectionName(sender)} connection] Recovery Succeeded");
        }

        private void Connection_ConnectionUnblocked(object sender, EventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{GetConnectionName(sender)} connection] Unblocked");
        }

        private void Connection_ConnectionBlocked(object sender, Client.Events.ConnectionBlockedEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{GetConnectionName(sender)} connection] Blocked");
        }

        private void Connection_CallbackException(object sender, Client.Events.CallbackExceptionEventArgs e)
        {
            _logger?.LogError(e.Exception, $"RabbitMQAbstraction[{GetConnectionName(sender)} connection] CallbackException. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void Connection_ConnectionRecoveryError(object sender, Client.Events.ConnectionRecoveryErrorEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{GetConnectionName(sender)} connection] Recovery Error. Message: {e.Exception.Message}{Environment.NewLine}Stacktrace: {e.Exception.StackTrace}");
        }

        private void Connection_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQAbstraction[{GetConnectionName(sender)} connection] Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText}");
        }

        public void Dispose()
        {
            foreach (var connection in Connections)
            {
                UnsubscribeConnectionEvents(connection);
                connection.Close();
                connection.Dispose();
            }
        }
    }
}