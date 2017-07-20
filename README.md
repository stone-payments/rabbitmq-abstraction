# rabbitmq-abstraction
## RabbitMQ Abstraction implements messaging, workflow and queue infrastructure management, focusing on simplicity and extending the original RabbitMQ.Client functionalities, while keeping .Net Standard 1.6 compatibility.

Build | Nuget | Coverage %
------|-------|-----------
|[![Build status](https://ci.appveyor.com/api/projects/status/2aj4ceuk47bl0kct/branch/master?svg=true)](https://ci.appveyor.com/project/rscouto/rabbitmq-abstraction/branch/master)| NugetPack|[![Coverage Status](https://coveralls.io/repos/github/stone-payments/rabbitmq-abstraction/badge.svg?branch=master)](https://coveralls.io/github/stone-payments/rabbitmq-abstraction?branch=master)

### Install
-------
    PM> Install-Package Stone.RabbitMQ.Abstraction


### How To Use
-------

#### Client Connect Exemple
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    //Do something
}
```

#### Publish
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    queueClient.Publish<string>("someExchange",
                                "someRoute",
                                "someObjectOfAnyReferenceType");
}
```

#### BatchPublish
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    queueClient.BatchPublish<string>("someExchange", 
                                     "someRoute", 
                                      new[] {
                                        "someObjectOfAnyReferenceType",
                                        "otherObjectOfAnyReferenceType"
                                    });
}
```

#### SimpleConsume
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var simpleMessageProcessingWorker = await SimpleMessageProcessingWorker<string>
            .CreateAndStartAsync(queueClient,
                                 "someQueue",
                                 Console.WriteLine,    
                                 CancellationToken.None)
            .ConfigureAwait(false);

    simpleMessageProcessingWorker.Stop();
}
```

#### AsyncSimpleConsume
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var simpleAsyncMessageProcessingWorker = await SimpleAsyncMessageProcessingWorker<string>
            .CreateAndStartAsync(queueClient,
                                 "someQueue", 
                                 (message, innerCancellationToken) =>
                                    {
                                        Console.WriteLine(message);
                                        return Task.FromResult(0);
                                    },
                                 TimeSpan.FromSeconds(10),
                                 CancellationToken.None)
            .ConfigureAwait(false);

    simpleAsyncMessageProcessingWorker.Stop();
}
```

#### AdvancedConsume
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var advancedMessageProcessingWorker = await AdvancedMessageProcessingWorker<string>
            .CreateAndStartAsync(queueClient,
                                 "someQueue",
                                 Console.WriteLine,
                                 CancellationToken.None)
            .ConfigureAwait(false);

    advancedMessageProcessingWorker.Stop();
}
```

#### AsyncAdvancedConsume
```csharp
using (var queueClient = new RabbitMQClient("localhost", 5672, "user", "password", "virtualHost"))
{
    var advancedAsyncMessageProcessingWorker = await AdvancedAsyncMessageProcessingWorker<string>
            .CreateAndStartAsync(queueClient,
                                 "someQueue",
                                 (message, innerCancellationToken) =>
                                    {
                                        Console.WriteLine(message);
                                        return Task.FromResult(0);
                                    },
                                 TimeSpan.FromSeconds(10),
                                 CancellationToken.None)
            .ConfigureAwait(false);;

    advancedAsyncMessageProcessingWorker.Stop();
}
```