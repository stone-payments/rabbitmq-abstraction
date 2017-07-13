# rabbitmq-abstraction

**Client**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  //Do something
}
```

**Publish**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  queueClient.Publish<string>("someExchange", "someRoute", "someObjectOfAnyReferenceType");
}
```

**BatchPublish**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  queueClient.BatchPublish<string>("someExchange", "someRoute", 
    new[] {"someObjectOfAnyReferenceType", "otherObjectOfAnyReferenceType"});
}
```

**SimpleConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var simpleMessageProcessingWorker = await SimpleMessageProcessingWorker<string>.CreateAndStartAsync(queueClient, "someQueue",
        Console.WriteLine, CancellationToken.None);

    simpleMessageProcessingWorker.Stop();
}
```

**AsyncSimpleConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var simpleAsyncMessageProcessingWorker = await SimpleAsyncMessageProcessingWorker<string>.CreateAndStartAsync(queueClient,
        "someQueue", (message, innerCancellationToken) =>
        {
            Console.WriteLine(message);

            return Task.FromResult(0);
        }, TimeSpan.FromSeconds(10), CancellationToken.None);

    simpleAsyncMessageProcessingWorker.Stop();
}
```

**AdvancedConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var advancedMessageProcessingWorker = await AdvancedMessageProcessingWorker<string>.CreateAndStartAsync(queueClient,
        "someQueue", Console.WriteLine, CancellationToken.None);

    advancedMessageProcessingWorker.Stop();
}
```

**AsyncAdvancedConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var advancedAsyncMessageProcessingWorker = await AdvancedAsyncMessageProcessingWorker<string>.CreateAndStartAsync(queueClient,
        "someQueue", (message, innerCancellationToken) =>
        {
            Console.WriteLine(message);

            return Task.FromResult(0);
        }, TimeSpan.FromSeconds(10), CancellationToken.None);

    advancedAsyncMessageProcessingWorker.Stop();
}
```
