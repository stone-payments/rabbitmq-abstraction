using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQ.Abstraction
{
    public static class AsyncExtensions
    {
        public static Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body, int degreeOfParallelism = 0)
        {
            if (degreeOfParallelism == 0)
            {
                degreeOfParallelism = Environment.ProcessorCount;
            }

            return Task.WhenAll(
                Partitioner.Create(source).GetPartitions(degreeOfParallelism).Select(partition =>
                    Task.Run(async delegate
                    {
                        var exceptions = new List<Exception>();

                        using (partition)
                        {
                            while (partition.MoveNext())
                            {
                                try
                                {
                                    await body(partition.Current);
                                }
                                catch (Exception e)
                                {
                                    exceptions.Add(e);
                                }
                            }
                        }

                        if (exceptions.Count != 0)
                        {
                            throw new AggregateException(exceptions);
                        }
                    })));
        }
    }
}
