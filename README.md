# Cleipnir's Resilient Functions
**Realizing the saga-pattern by providing a simple way to ensure your code gets run - until you say it is done!**

Resilient Functions is a simple and intuitive .NET framework for managing the execution of functions which must complete in their entirety despite: failures, restarts, deployments, versioning etc. 

It automatically retries a function invocation until it completes potentially across process restarts and physical nodes. 

The framework also supports postponing/suspending invocations or failing invocations for manually handling. Furthermore, versioning is natively supported.

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances.

Crucially, all this allows the **saga pattern** to be implemented in a simple yet powerful way. 

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

## Sections
* [Getting Started](#getting-started)
* [Show me more code](#show-me-more-code)
  * [Hello World](#hello-world) 
  * [HTTP-call & Database](#http-call--database)
  * [Sending customer emails](#sending-customer-emails)

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
var rAction = functionsRegistry.RegisterAction(
  functionTypeId: "OrderProcessor",
  async (Order order, OrderScrapbook scrapbook) => 
  {
    await _paymentProviderClient.Reserve(order.CustomerId, scrapbook.TransactionId, order.TotalPrice);

    await scrapbook.DoAtMostOnce(
      workStatus: s => s.ProductsShippedStatus,
      work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds)
    );

    await _paymentProviderClient.Capture(scrapbook.TransactionId);
    await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);
  }
);

var order = new Order(
  OrderId: "MK-4321",
  CustomerId: Guid.NewGuid(),
  ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
  TotalPrice: 123.5M
);
await rAction.Invoke(order.OrderId, order);
```

Congrats, any non-completed Order flows are now automatically restarted by the framework.

### Message-based solution:
It is also possible to implement message-based flows using the framework.
I.e. awaiting 2 external messages before completing an invocation can be accomplished as follows:
```csharp
 var rAction = functionsRegistry.RegisterAction(
  functionTypeId: "MessageWaitingFunc",
  async (string param, Context context) => 
  {
    var messages = await context.Messages;
    await messages
      .OfTypes<FundsReserved, InventoryLocked>()
      .Take(2)
      .ToList();
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
  functionTypeId: "HelloWorld",
  inner: (string param) => param.ToUpper()
).Invoke;

var returned = await rFunc(functionInstanceId: "", param: "hello world");
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
    functionTypeId: "HttpAndDatabaseSaga",
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
  await rAction(functionInstanceId: id.ToString(), param: id);
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Samples/Sample.ConsoleApp/Simple/SimpleHttpAndDbExample.cs)

### Sending customer emails
Consider a travel agency which wants to send a promotional email to its customers:
```csharp
public static class EmailSenderSaga
{
  public static async Task Start(MailAndRecipients mailAndRecipients, Scrapbook scrapbook)
  {
    var (recipients, subject, content) = mailAndRecipients;

    using var client = new SmtpClient();
    await client.ConnectAsync("mail.smtpbucket.com", 8025);
        
    for (var atRecipient = scrapbook.AtRecipient; atRecipient < mailAndRecipients.Recipients.Count; atRecipient++)
    {
      var recipient = recipients[atRecipient];
      var message = new MimeMessage();
      message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
      message.From.Add(new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

      message.Subject = subject;
      message.Body = new TextPart(TextFormat.Html) { Text = content };
      await client.SendAsync(message);

      scrapbook.AtRecipient = atRecipient;
      await scrapbook.Save();
    }
  }

  public class Scrapbook : RScrapbook
  {
    public int AtRecipient { get; set; }
  }
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/tree/main/Samples/Sample.ConsoleApp/EmailOffers)
