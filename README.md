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
  var simpleMessageProcessingWorker = new SimpleMessageProcessingWorker<string>(queueClient, "someQueue",
    Console.WriteLine);

  simpleMessageProcessingWorker.Stop();
}
```

**AsyncSimpleConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  var cancellationToken = new CancellationToken();

  var simpleAsyncMessageProcessingWorker = new SimpleAsyncMessageProcessingWorker<string>(queueClient, 
      "someQueue", (message, innerCancellationToken) =>
      {
          Console.WriteLine(message);

          return Task.FromResult(0);
      }, cancellationToken);

  simpleAsyncMessageProcessingWorker.Stop();
}
```

**AdvancedConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  var advancedMessageProcessingWorker = new AdvancedMessageProcessingWorker<string>(queueClient,
      "someQueue", Console.WriteLine);

  advancedMessageProcessingWorker.Stop();
}
```

**AsyncAdvancedConsume**
```C#
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
  var advancedAsyncMessageProcessingWorker = new AdvancedAsyncMessageProcessingWorker<string>(queueClient,
      "someQueue", (message, innerCancellationToken) =>
      {
          Console.WriteLine(message);

          return Task.FromResult(0);
      }, cancellationToken);

  advancedAsyncMessageProcessingWorker.Stop();
}
```
