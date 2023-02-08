# Cleipnir's Resilient Functions
**Realizing the saga-pattern by providing a simple way to ensure your code gets run - until you say it is done!**

Resilient Functions is a simple and intuitive .NET framework for managing the execution of functions which must complete in their entirety despite: failures, restarts, deployments, versioning etc. 

It automatically retries a function invocation until it completes potentially across process restarts and physical nodes. 

The framework also supports postponing/suspending invocations or failing invocations for manually handling. Furthermore, versioning is natively supported.

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances.

Crucially, all this allows the **saga pattern** to be implemented in a simple yet powerful way. 

Out-of-the-box you also get:
* ASP.NET Core integration with graceful shutdown support
* synchronized invocation across multiple process instances
* cloud independence & support for multiple databases
* simple debuggability & testability
* easy versioning of functions
* add custom middleware to address cross-cutting corcerns

If you like a slide deck can be found [here](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Documentation/raw/main/Presentation.pdf)  

| What it is not? |
| --- |
| Unlike other saga frameworks Resilient Functions does not require a message-broker to operate.<br /> It is a fully self-contained solution - which operates on top of a database of choice or in-memory when testing.<br />|

## Sections
* [Getting Started](#getting-started)
* [Learning by doing](#learning-by-doing)
* [Show me more code](#show-me-more-code)
  * [Hello World](#hello-world) 
  * [HTTP-call & Database](#http-call--database)
  * [Sending customer emails](#sending-customer-emails)

## Getting Started
Only three steps needs to be performed to get started.

Firstly, install the relevant nuget package (using either Postgres, SqlServer or MySQL as persistence layer) into a ASP.NET Core project. 
```powershell
Install-Package Cleipnir.ResilientFunctions.AspNetCore.Postgres
```
or
```powershell
Install-Package Cleipnir.ResilientFunctions.AspNetCore.SqlServer
```
or
```powershell
Install-Package Cleipnir.ResilientFunctions.AspNetCore.MySql
```

Secondly, add the following to ASP.NET Core's `Program.cs`:
```csharp
builder.Services.UseResilientFunctions( 
  connectionString,
  _ => new Options(
    unhandledExceptionHandler: rfe => Log.Logger.Error(rfe, "ResilientFrameworkException occured")
  )
);
```

Finally, register a function with the framework ([source code](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing/blob/main/Rpc/Version_2/Ordering/OrderProcessor.cs)):
```csharp
public class OrderProcessor : IRegisterRFuncOnInstantiation
{
  private readonly RAction<Order> _rAction;

  public OrderProcessor(RFunctions rFunctions)
  {
    _rAction = rFunctions
      .RegisterMethod<Inner>()
      .RegisterAction<Order>(
        functionTypeId: nameof(OrderProcessor),
        inner => inner.ProcessOrder
      );
  }

  public Task ProcessOrder(Order order)
    => _rAction.Invoke(
          functionInstanceId: order.OrderId,
          param: order
       );
    
  public class Inner
  {
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;

    public Inner(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
    {
      _paymentProviderClient = paymentProviderClient;
      _emailClient = emailClient;
      _logisticsClient = logisticsClient;
    }

    public async Task ProcessOrder(Order order)
    {
      Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

      await _paymentProviderClient.Reserve(order.TransactionId, order.CustomerId, order.TotalPrice);
      await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
      await _paymentProviderClient.Capture(order.TransactionId);
      await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

      Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' completed");
    }    
  }
}
```

Alternatively, registering a function for a message-based solution ([source code](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing/blob/main/Messaging/Version_0/Ordering/OrderProcessor.cs)) can be accomplished as follows:
```csharp
public class OrderProcessor : IRegisterRFuncOnInstantiation
{
  private RAction<Order, Scrapbook> RAction { get; }

  public OrderProcessor(RFunctions rFunctions, MessageBroker messageBroker)
  {
    RAction = rFunctions
      .RegisterMethod<Inner>()
      .RegisterAction<Order, Scrapbook>(
        nameof(OrderProcessor),
        inner => inner.ProcessOrder
      );

    messageBroker.Subscribe(async msg =>
    {
      switch (msg)
      {
        case FundsCaptured e:
          await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsCaptured)}.{e.OrderId}");
          break;
        case FundsReservationCancelled e:
          await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReservationCancelled)}.{e.OrderId}");
          break;
        case FundsReserved e:
          await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReserved)}.{e.OrderId}");
          break;
         case OrderConfirmationEmailSent e:
           await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(OrderConfirmationEmailSent)}.{e.OrderId}");
           break;
         case ProductsShipped e:
           await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(ProductsShipped)}.{e.OrderId}");
           break;
         default: return;
      }
    });
  }
    
  public Task ProcessOrder(Order order) => RAction.Invoke(order.OrderId, order);

  public class Inner
  {
    private readonly MessageBroker _messageBroker;

    public Inner(MessageBroker messageBroker) => _messageBroker = messageBroker;

    public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
    {
      Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
      using var eventSource = await context.EventSource;

      await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
      await eventSource.NextOfType<FundsReserved>(maxWait: TimeSpan.FromSeconds(5));
            
      await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
      await eventSource.NextOfType<ProductsShipped>(maxWait: TimeSpan.FromSeconds(5));
            
      await _messageBroker.Send(new CaptureFunds(order.OrderId, order.CustomerId, scrapbook.TransactionId));
      await eventSource.NextOfType<FundsCaptured>(maxWait: TimeSpan.FromSeconds(5));

      await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
      await eventSource.NextOfType<OrderConfirmationEmailSent>(maxWait: TimeSpan.FromSeconds(5));

      Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
    }        
  }

  public class Scrapbook : RScrapbook
  {
    public Guid TransactionId { get; set; } = Guid.NewGuid();
  }
}
```

## Learning by doing
Sometimes the simplest approach to understand something is to see it in action.

During this chapter we will work our way step-by-step from a simple order-flow in an ordinary ASP.NET Core project into a fully resilient and robust order-flow implementation supported by the framework. 

Resilient Functions supports both RPC-based and messaging-based communication. Solutions to both approaches are presented in this chapter.  

All source code examples together with a ready to run web-api application can be found [here](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing).

Our starting point is the following 4-step order-flow:
1. Reserve funds from PaymentProvider (i.e. customer’s credit card)
2. Ship products to customer (get delivery confirmation from logistics service)
3. Capture funds from PaymentProvider (redeem credit card reservation)
4. Email order confirmation to customer


In ordinary C# code this translates to ([source code](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing/blob/main/Rpc/Version_0/Ordering/OrderProcessor.cs)):
```csharp
public async Task ProcessOrder(Order order)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

  var transactionId = Guid.NewGuid();
  await _paymentProviderClient.Reserve(transactionId, order.CustomerId, order.TotalPrice);
  await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
  await _paymentProviderClient.Capture(transactionId);
  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' completed");
}
```

Currently, the order-flow is not robust against *crashes* or *restarts*. 

For instance if the process crashes just before capturing the funds from the payment provider then the ordered products are shipped to the customer but without anything being deducted from the customer’s credit card. 
Not an ideal situation for the business. 
No matter how we rearrange the flow a crash might lead to either situation:
- products are shipped to the customer without payment being deducted from the customer’s credit card
- payment is deducted from the customer’s credit card but products are never shipped

**Ensuring restart on crashes or restarts:**

Thus, to rectify the situation we must ensure that the flow is *restarted* if it did not complete in a previous invocation. 
In Cleipnir this is accomplished by registering the order processing function with the framework.

### RPC-solution
Registering a function with the framework can be done by changing the code in the following way ([source code](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing/blob/main/Rpc/Version_1/Ordering/OrderProcessor.cs)):

```csharp
public class OrderProcessor : IRegisterRFuncOnInstantiation
{
  private RAction.Invoke<Order, RScrapbook> RAction { get; }

  public OrderProcessor(RFunctions rFunctions)
  {
    var registration = rFunctions
      .RegisterMethod<Inner>()
      .RegisterAction<Order>(
        nameof(OrderProcessor),
        inner => inner.ProcessOrder
      );

     RAction = registration.Invoke;
  }

  public Task ProcessOrder(Order order) => RAction.Invoke(order.OrderId, order);

  public class Inner
  {
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;

    public Inner(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
    {
      _paymentProviderClient = paymentProviderClient;
      _emailClient = emailClient;
      _logisticsClient = logisticsClient;
    }

    public async Task ProcessOrder(Order order)
    {
      Log.Logger.ForContext<OrderProcessor>().Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

      var transactionId = Guid.Empty;
      await _paymentProviderClient.Reserve(order.CustomerId, transactionId, order.TotalPrice);
      await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
      await _paymentProviderClient.Capture(transactionId);
      await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

      Log.Logger.ForContext<OrderProcessor>().Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' completed");
    }       
  }
}
```

Sometimes simply wrapping a business flow inside the framework is enough. 

This would be the case if all the steps in the flow were idempotent. In that situation it is fine to call an endpoint multiple times without causing unintended side-effects.

#### At-least-once & Idempotency

However, in the order-flow presented here this is not the case. 

The payment provider requires the caller to provide a transaction-id. Thus, the same transaction-id must be provided when re-executing the flow. 

In Cleipnir this challenge is solved by using a *scrapbook*. 

A scrapbook is a user-defined sub-type which holds state useful when/if the function invocation is retried. Using it one can ensure that the same transaction id is always used for the same order in the following way:


```csharp 
public class Scrapbook : RScrapbook
{
  public Guid TransactionId { get; set; }
}
```

```csharp
public async Task ProcessOrder(Order order, Scrapbook scrapbook)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

  if (scrapbook.TransactionId == Guid.Empty)
  {
    scrapbook.TransactionId = Guid.NewGuid();
    await scrapbook.Save();
  }
  
  await _paymentProviderClient.Reserve(scrapbook.TransactionId, order.CustomerId, order.TotalPrice);
  await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
  await _paymentProviderClient.Capture(scrapbook.TransactionId);
  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
}
```

Essentially, a scrapbook is simply a user-defined poco-class which can be saved on demand. 

In the example given, the code may be simplified further, as the scrapbook is also saved by the framework before the first function invocation begins. I.e.

```csharp
public class Scrapbook : RScrapbook
{
   public Guid TransactionId { get; set; } = Guid.NewGuid();
}
```

```csharp
public async Task ProcessOrder(Order order, Scrapbook scrapbook)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

  await _paymentProviderClient.Reserve(scrapbook.TransactionId, order.CustomerId, order.TotalPrice);
  await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
  await _paymentProviderClient.Capture(scrapbook.TransactionId);
  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
}
```

#### At-most-once API:

For the sake of presenting the framework’s versatility let us assume that the logistics’ API is *not* idempotent and that it is out of our control to change that. 

Thus, every time a successful call is made to the logistics service the content of the order is shipped to the customer. 

As a result the order-flow must fail if it is restarted and:
* a request was previously sent to logistics-service 
* but no response was received. 
 
This can again be accomplished by using the scrapbook:

```csharp
public class Scrapbook : RScrapbook
{
  public Guid TransactionId { get; set; } = Guid.NewGuid();
  public WorkStatus ProductsShippedStatus { get; set; }
}
```

```csharp
public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
  
  await _paymentProviderClient.Reserve(order.CustomerId, scrapbook.TransactionId, order.TotalPrice);

  if (scrapbook.ProductsShippedStatus == WorkStatus.NotStarted)
  {
    scrapbook.ProductsShippedStatus = WorkStatus.Started;
    await scrapbook.Save();

    await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);

    scrapbook.ProductsShippedStatus = WorkStatus.Completed;
    await scrapbook.Save();
  }
  else if (scrapbook.ProductsShippedStatus == WorkStatus.Started)
    throw new InvalidOperationException("A request to the logistics service was previously sent without a response being received before crash");

  await _paymentProviderClient.Capture(scrapbook.TransactionId);           

  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
}  
```

A *failed/exception throwing* function is not automatically retried by the framework. 

Instead it must be manually re-invoked by using the function instance’s associated control-panel. 

**Control Panel:**

Using the function’s control panel both the parameter and scrapbook may be changed before the function is retried. 

For instance, assuming it is determined that the products where not shipped for a certain order, then the following code re-invokes the order with the scrapbook changed accordingly. 

```csharp
private readonly RAction<Order, Scrapbook> _rAction;
private async Task Retry(string orderId)
{
  var controlPanel = await _rAction.ControlPanels.For(orderId);
  controlPanel!.Scrapbook.ProductsShippedStatus = WorkStatus.Completed;
  await controlPanel.ReInvoke();
}
```

**At-most-once convenience syntax:**

The framework has built-in support for the at-most-once (and at-least-once) pattern presented above using the scrapbook as follows:

```csharp
public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
  
  await _paymentProviderClient.Reserve(order.CustomerId, scrapbook.TransactionId, order.TotalPrice);
  
  await scrapbook.DoAtMostOnce(
    workStatus: s => s.ProductsShippedStatus,
    work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds)
  );

  await _paymentProviderClient.Capture(scrapbook.TransactionId);           

  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
}
```

#### Testing

It is simple to test a resilient function as:
* it is just a matter of creating an instance of the type containing the resilient function 
* and invoking the method in question.

Verifying that the order processing fails:
* when retried
* and a request in a previous invocation has been sent to the logistics service 
* but no reply was received 

Can be accomplished as follows:

```csharp
[TestMethod]
public async Task OrderProcessorFailsOnRetryWhenLogisticsWorkHasStartedButNotCompleted()
{
  var sut = new OrderProcessor.Inner(
    PaymentProviderClientStub,
    EmailClientStub,
    LogisticsClientStub
  );

  var order = new Order(
    OrderId: "MK-54321",
    CustomerId: Guid.NewGuid(),
    ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
    TotalPrice: 120M
  );
  var scrapbookSaved = false;
  var scrapbook = new OrderProcessor.Scrapbook
  {
    TransactionId = Guid.NewGuid(),
    ProductsShippedStatus = WorkStatus.Started
  };
  scrapbook.Initialize(onSave: () => { scrapbookSaved = true; return Task.CompletedTask; });

  await Should.ThrowAsync<InvalidOperationException>(() => sut.ProcessOrder(order, scrapbook));
  
  scrapbookSaved.ShouldBeFalse();
  scrapbook.ProductsShippedStatus.ShouldBe(WorkStatus.Started);
  EmailClientStub.SendOrderConfirmationInvocations.ShouldBeEmpty();
  LogisticsClientStub.ShipProductsInvocations.ShouldBeEmpty();
  PaymentProviderClientStub.ReserveInvocations.Count.ShouldBe(1);
  PaymentProviderClientStub.CaptureInvocations.ShouldBeEmpty();
  PaymentProviderClientStub.CancelReservationInvocations.ShouldBeEmpty();
}
```

### Message-based Solution 
Message- or event-driven system are omnipresent in enterprise architectures today. 

They fundamentally differ from RPC-based in that:
* messages related to the same order are not delivered to the same process. 

This has huge implications in how a saga-flow is implemented and as a result a simple sequential flow - as in the case of the order-flow:
* becomes fragmented and hard to reason about
* inefficient - each time a message is received the entire state must be reestablished
* inflexible

Cleipnir Resilient Functions takes a novel approach by piggy-backing on the features described so far and using event-sourcing and reactive programming together to form a simple and extremely useful abstraction.

As a result order-flow can be implemented as follows ([source-code](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Sample.OrderProcessing/blob/main/Messaging/Version_0/Ordering/OrderProcessor.cs)):

```csharp
public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
  using var eventSource = await context.EventSource;

  await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
  await eventSource.NextOfType<FundsReserved>();
            
  await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
  await eventSource.NextOfType<ProductsShipped>();
            
  await _messageBroker.Send(new CaptureFunds(order.OrderId, order.CustomerId, scrapbook.TransactionId));
  await eventSource.NextOfType<FundsCaptured>();

  await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
  await eventSource.NextOfType<OrderConfirmationEmailSent>();

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");      
}
```
There is a bit more going on in the example above compared to the previous RPC-example. 
However, the flow is actually very similar to RPC-based. It is sequential and robust. If the flow crashes and is restarted it will continue from the point it got to before the crash.

It is noted that the message broker in the example is just a stand-in - thus not a framework concept - for RabbitMQ, Kafka or some other messaging infrastructure client. 

In a real application the message broker would be replaced with the actual way the application broadcasts a message/event to other services.

Furthermore, each resilient function has an associated private **event source**. When events are received from the network they can be placed into the relevant resilient function's event source - thereby allowing the function to continue its flow. 

| Did you know? |
| --- |
| The framework allows awaiting events both in-memory or suspending the invocation until an event has been appended to the event source. </br>Thus, allowing the developer to find the sweet-spot per use-case between performance and releasing resources.|


## Show me more code
Resilient Functions does not require ASP.NET Core to operate. Thus, getting started in a console-application can be accomplished only using the core package:

```powershell
Install-Package Cleipnir.ResilientFunctions
```

Or `Cleipnir.ResilientFunctions.PostgresSQL`, `Cleipnir.ResilientFunctions.SqlServer` or `Cleipnir.ResilientFunctions.MySQL` if persistence is required.

In the following chapter several stand-alone examples are presented. 

### Hello-World
Firstly, the compulsory, ‘*hello world*’-example can be realized as follows:

```csharp
var store = new InMemoryFunctionStore();
var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);

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
  var functions = new RFunctions(store, new Settings(UnhandledExceptionHandler: Console.WriteLine));
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
