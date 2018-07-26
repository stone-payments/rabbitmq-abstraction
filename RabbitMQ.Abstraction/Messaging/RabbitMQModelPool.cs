using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQModelPool : IDisposable
    {
        private ConcurrentQueue<IModel> _models;

        private readonly uint _poolSize;

        private readonly Func<IModel> _createModelFunc;

        private int _inUseModelCount;

        public RabbitMQModelPool(Func<IModel> createModelFunc, uint poolSize = 1)
        {
            _poolSize = poolSize == 0 ? 1 : poolSize;
            _createModelFunc = createModelFunc;
            _inUseModelCount = 0;

            _models = new ConcurrentQueue<IModel>();

            EnsurePoolSize();
        }

        private void EnsurePoolSize()
        {
            lock (_models)
            {
                _models = new ConcurrentQueue<IModel>(_models.Where(m => m.IsOpen).Take((int)_poolSize));

                var newModelsNeeded = _poolSize - (_models.Count + _inUseModelCount);

                for (var i = 0; i < newModelsNeeded; i++)
                {
                    _models.Enqueue(new RabbitMQModel(_createModelFunc(), model =>
                    {
                        Interlocked.Decrement(ref _inUseModelCount);
                        _models.Enqueue(model);
                    }));
                }
            }
        }

        public async Task<IModel> GetModelAsync()
        {
            bool success;
            IModel elegibleModel;

            do
            {
                success = _models.TryDequeue(out elegibleModel);

                if (!success)
                {
                    if (elegibleModel.IsClosed)
                    {
                        _models.Enqueue(elegibleModel);
                    }

                    EnsurePoolSize();

                    await Task.Delay(TimeSpan.FromMilliseconds(50));
                }
            } while (!success || elegibleModel.IsClosed);

            Interlocked.Increment(ref _inUseModelCount);

            return elegibleModel;
        }

        public void Dispose()
        {
            lock (_models)
            {
                foreach (var model in _models.Where(c => c.IsOpen))
                {
                    model.Close();
                }

                _models = null;
            }
        }
    }
}
