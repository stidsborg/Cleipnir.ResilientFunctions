[![.NET](https://github.com/stidsborg/Cleipnir.ResilientFunctions/actions/workflows/dotnet.yml/badge.svg?no-cache)](https://github.com/stidsborg/Cleipnir.ResilientFunctions/actions/workflows/dotnet.yml)
[![NuGet](https://img.shields.io/nuget/dt/Cleipnir.ResilientFunctions.svg)](https://www.nuget.org/packages/Cleipnir.ResilientFunctions)
[![NuGet](https://img.shields.io/nuget/vpre/Cleipnir.ResilientFunctions.svg)](https://www.nuget.org/packages/Cleipnir.ResilientFunctions)
[![Changelog](https://img.shields.io/badge/-Changelog-darkred)](./CHANGELOG.md)

<p align="center">
  <img src="https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Docs/cleipnir.png" alt="logo" />
  <br>
  Simply making fault-tolerant code simple
  <br>
</p>

# Cleipnir.Flows
Looking for Cleipnir.Flows, which provides better support for ASP.NET and generic hosted services?

[Github Repo](http://cleipnir.net/)


# Cleipnir Resilient Functions
**Providing a simple way to ensure your code gets run - until you say it is done!**

Resilient Functions is a simple and intuitive .NET framework for managing the execution of functions which must complete in their entirety despite: failures, restarts, deployments, versioning etc. 

It automatically retries a function invocation until it completes potentially across process restarts and physical nodes. 

The framework also supports postponing/suspending invocations or failing invocations for manually handling. Furthermore, versioning is natively supported.

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances.

Crucially, all this allows the **saga pattern / process manager pattern** to be implemented in a simple yet powerful way. 

Out-of-the-box you also get:
* synchronized invocation across multiple process instances
* cloud independence & support for multiple databases
* simple debuggability & testability
* easy versioning of functions
* native support for rpc and message-based communication
* graceful-shutdown

| What it is not? |
| --- |
| Unlike other saga frameworks Resilient Functions does not require a message-broker to operate.<br /> It is a fully self-contained solution - which operates on top of a database of choice or in-memory when testing.<br />|

## Getting Started
Only three steps needs to be performed to get started.

Firstly, install the relevant nuget package (using either Postgres, SqlServer, MySQL or Azure Blob-storage as persistence layer). I.e.
```console
dotnet add package Cleipnir.ResilientFunctions.PostgreSQL
```

Secondly, setup the framework:
```csharp
 var store = new PostgreSqlFunctionStore(ConnStr);
 await store.Initialize();
 var functionsRegistry = new FunctionsRegistry(
   store,
   new Settings(
     unhandledExceptionHandler: e => Console.WriteLine($"Unhandled framework exception occured: '{e}'"),
     leaseLength: TimeSpan.FromSeconds(5)
   )
 );
```

Finally, register and invoke a function using the framework:
```csharp
var actionRegistration = functionsRegistry.RegisterAction(
  flowType: "OrderProcessor",
  async (Order order, Workflow workflow) => 
  { 
    var effect = workflow.Effect;  
    var transactionId = effect.Capture("TransactionId", Guid.NewGuid);    
    await _paymentProviderClient.Reserve(order.CustomerId, transactionId, order.TotalPrice);

    await effect.Capture(
      "ShipProducts",
      work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds),
      ResiliencyLevel.AtMostOnce
    );

    await _paymentProviderClient.Capture(transactionId);
    await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);
  }
);

var order = new Order(
  OrderId: "MK-4321",
  CustomerId: Guid.NewGuid(),
  ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
  TotalPrice: 123.5M
);

await actionRegistration.Invoke(order.OrderId, order);
```

Congrats, any non-completed Order flows are now automatically restarted by the framework.

### Message-based solution:
It is also possible to implement message-based flows using the framework.
I.e. awaiting 2 external messages before completing an invocation can be accomplished as follows:
```csharp
 var rAction = functionsRegistry.RegisterAction(
  flowType: "MessageWaitingFunc",
  async (string param, Workflow workflow) => 
  {
    var messages = await workflow.Messages;
    await messages
      .OfTypes<FundsReserved, InventoryLocked>()
      .Take(2)
      .Completion();
  }
);
```

## Show me more code
In the following chapter several stand-alone examples are presented. 

### Hello-World
Firstly, the compulsory, ‘*hello world*’-example can be realized as follows:

```csharp
var store = new InMemoryFunctionStore();
var functions = new FunctionsRegistry(store, unhandledExceptionHandler: Console.WriteLine);

var rFunc = functions.RegisterFunc(
  flowType: "HelloWorld",
  inner: (string param) => param.ToUpper()
).Invoke;

var returned = await rFunc(flowInstance: "", param: "hello world");
Console.WriteLine($"Returned: '{returned}'");
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Samples/Sample.ConsoleApp/Simple/HelloWorldExample.cs)

### HTTP-call & database
Allright, not useful, here are a couple of simple, but common, use-cases.

Invoking a HTTP-endpoint and storing the response in a database table:
```csharp
public static async Task RegisterAndInvoke(IDbConnection connection, IFunctionStore store)
{
  var functions = new FunctionsRegistry(store, new Settings(UnhandledExceptionHandler: Console.WriteLine));
  var httpClient = new HttpClient();

  var rAction = functions.RegisterAction(
    flowType: "HttpAndDatabaseSaga",
    inner: async (Guid id) =>
    {
      var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
      response.EnsureSuccessStatusCode();
      var content = await response.Content.ReadAsStringAsync();
      await connection.ExecuteAsync(
        "UPDATE Entity SET State=@State WHERE Id=@Id",
        new {State = content, Id = id}
      );
    }).Invoke;

  var id = Guid.NewGuid();
  await rAction(flowInstance: id.ToString(), param: id);
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Samples/Sample.ConsoleApp/Simple/SimpleHttpAndDbExample.cs)

### Sending customer emails
Consider a travel agency which wants to send a promotional email to its customers:
```csharp
public static class EmailSenderSaga
{
  public static async Task Start(MailAndRecipients mailAndRecipients, Workflow workflow)
  {
    var state = workflow.States.CreateOrGet<State>();  
    var (recipients, subject, content) = mailAndRecipients;

    using var client = new SmtpClient();
    await client.ConnectAsync("mail.smtpbucket.com", 8025);
        
    for (var atRecipient = state.AtRecipient; atRecipient < mailAndRecipients.Recipients.Count; atRecipient++)
    {
      var recipient = recipients[atRecipient];
      var message = new MimeMessage();
      message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
      message.From.Add(new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

      message.Subject = subject;
      message.Body = new TextPart(TextFormat.Html) { Text = content };
      await client.SendAsync(message);

      state.AtRecipient = atRecipient;
      await state.Save();
    }
  }

  public class State : FlowState
  {
    public int AtRecipient { get; set; }
  }
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/tree/main/Samples/Sample.ConsoleApp/EmailOffers)
