using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    //public class RabbitMQConnectionPool : IDisposable
    //{
    //    public readonly ConnectionFactory ConnectionFactory;

    //    private readonly List<IConnection> _connections;

    //    private readonly uint _poolSize;

    //    public RabbitMQConnectionPool(ConnectionFactory connectionFactory)
    //    {
    //        ConnectionFactory = connectionFactory;

    //        _connections = new List<IConnection>();

    //        if (_poolSize == 0)
    //        {
    //            _poolSize = 1;
    //        }

    //        EnsurePoolSize();
    //    }

    //    public async Task<IConnection> GetConnectionAsync()
    //    {
    //        EnsurePoolSize();

    //        IConnection elegibleConnection;

    //        lock(_connections)
    //        {
    //            elegibleConnection = _connections.FirstOrDefault(c => c.IsOpen);
    //        }

    //        if(elegibleConnection == null)
    //        {
    //            elegibleConnection = await GetConnectionAsync().ConfigureAwait(false);
    //        }

    //        var connection = await EnsureConnectionOpenAsync(elegibleConnection).ConfigureAwait(false);

    //        return connection;
    //    }

    //    private async Task<IConnection> EnsureConnectionOpenAsync(IConnection connection)
    //    {
    //        IConnection ensuredConnection;

    //        try
    //        {
    //            var model = connection.CreateModel();

    //            model.Dispose();

    //            ensuredConnection = connection;
    //        }
    //        catch (Exception)
    //        {
    //            //TODO: Review this lock
    //            lock (_connections)
    //            {
    //                _connections.Remove(connection);
    //            }

    //            await Task.Delay(TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);

    //            ensuredConnection = await GetConnectionAsync().ConfigureAwait(false);
    //        }
            
    //        return ensuredConnection;
    //    }

    //    private void EnsurePoolSize()
    //    {
    //        lock(_connections)
    //        {
    //            _connections.RemoveAll(c => c.IsOpen == false);

    //            var newConnectionsNeeded = Convert.ToInt32(_poolSize - _connections.Count);

    //            for (var i = 0; i < newConnectionsNeeded; i++)
    //            {
    //                _connections.Add(ConnectionFactory.CreateConnection());
    //            }
    //        }
    //    }

    //    public void Dispose()
    //    {
    //        lock(_connections)
    //        {
    //            foreach (var connection in _connections.Where(c => c.IsOpen))
    //            {
    //                connection.Close();
    //            }

    //            _connections.RemoveAll(c => c.IsOpen == false);
    //        }
    //    }
    //}
}
