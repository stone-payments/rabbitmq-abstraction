using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Messaging.Interfaces;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQModelPool : IDisposable
    {
        private ConcurrentQueue<IRabbitMQModel> _models;
        private readonly object _modelsLock = new object();

        private readonly uint _poolSize;

        private readonly Func<IModel> _createModelFunc;

        private int _inUseModelCount;

        public RabbitMQModelPool(Func<IModel> createModelFunc, uint poolSize = 1)
        {
            try
            {
                _poolSize = poolSize == 0 ? 1 : poolSize;
                _createModelFunc = createModelFunc;
                _inUseModelCount = 0;

                _models = new ConcurrentQueue<IRabbitMQModel>();

                EnsurePoolSize();
            }
            catch (Exception e)
            {
                throw;
            }
        }

        private void EnsurePoolSize()
        {
            lock (_modelsLock)
            {
                var openModels = new ConcurrentQueue<IRabbitMQModel>();

                foreach (var model in _models)
                {
                    if (model.IsOpen)
                    {
                        if (openModels.Count + _inUseModelCount < _poolSize)
                        {
                            openModels.Enqueue(model);
                        }
                        else
                        {
                            model.End();
                        }
                    }
                    else
                    {
                        model.Dispose();
                    }
                }

                _models = openModels;

                var newModelsNeeded = _poolSize - (_models.Count + _inUseModelCount);

                for (var i = 0; i < newModelsNeeded; i++)
                {
                    _models.Enqueue(new RabbitMQModel(_createModelFunc(), model =>
                    {
                        lock (_modelsLock)
                        {
                            _models.Enqueue(model);
                        }

                        Interlocked.Decrement(ref _inUseModelCount);
                    }, () => Interlocked.Decrement(ref _inUseModelCount)));
                }
            }
        }

        public async Task<IModel> GetModelAsync()
        {
            bool success;
            IRabbitMQModel elegibleModel;

            do
            {
                lock (_modelsLock)
                {
                    success = _models.TryDequeue(out elegibleModel);
                }

                if (success)
                {
                    if (elegibleModel.IsClosed)
                    {
                        elegibleModel.Dispose();

                        success = false;
                    }
                }
                else
                {
                    EnsurePoolSize();

                    await Task.Delay(TimeSpan.FromMilliseconds(50));
                }
            } while (!success);

            Interlocked.Increment(ref _inUseModelCount);

            return elegibleModel;
        }

        public void Dispose()
        {
            lock (_modelsLock)
            {
                foreach (var model in _models)
                {
                    model.End();
                }

                _models = null;
            }
        }
    }
}
