# Cleipnir's Resilient Functions
**Realizing the saga-pattern by simply providing a way to ensure your code gets run - until you say it is done!**

Resilient Functions is a simple and intuitive .NET framework for managing the execution of functions which must complete in their entirety despite: failures, restarts, deployments, versioning etc. 

It automatically retries a function invocation until it completes potentially across process restarts and physical nodes. 

The framework also supports postponing/suspending invocations or failing invocations for manually handling. Furthermore, versioning is natively supported.

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances.

Crucially, all this allows the **saga pattern** to be implemented in a simple yet powerful way. 

Out-of-the-box you also get:
* synchronized invocation across multiple process instances
* cloud independance & support for multiple databases
* simple debuggability & testability
* easy versioning of functions
* graceful shutdown
* add custom middleware to address cross-cutting corcerns

If you like a slide deck can be found [here](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Documentation/raw/main/Presentation.pdf)  

| What it is not? |
| --- |
| Resilient Functions is not a message-broker based solution. Meaning, it does not need a message-broker to operate.<br />|

## Sections
* [Getting Started](#getting-started)
* [Elevator Pitch](#elevator-pitch)
* [Show me more code](#show-me-more-code)
  * [Hello World](#hello-world) 
  * [HTTP-call & Database](#http-call--database)
  * [Sending customer emails](#sending-customer-emails)

## Getting Started
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

And add the following to ASP.NET Core's `Program.cs`:
```csharp
builder.Services.UseResilientFunctions( 
  connectionString,
  _ => new Options(
    unhandledExceptionHandler: rfe => Log.Logger.Error(rfe, "ResilientFrameworkException occured")
  )
);
```

Finally, register function with the framework:
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

## Elevator Pitch
Still curious - ok awesome! 

Our starting point is the following 4-step **order-flow**:
1. Reserve funds from PaymentProvider (i.e. customer’s credit card)
2. Ship products to customer (make call to the logistics service api)
3. Capture funds from PaymentProvider (redeem credit card reservation)
4. Email order confirmation to customer (using an external email service)

Let us assume that the Payment Provider requires a client generated transaction id to be sent along in all requests for a given order. 
Thus, allowing the Payment Provider to recognize if it has previously processed a given request. In effect making the payment provider api idempotent. 

In this scenario a robust order-flow can be realized using either a RPC or message-based approach as shown below:

<ins>RPC:</ins>
```csharp
public async Task ProcessOrder(Order order)
{
  Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            
  await _paymentProviderClient.Reserve(order.TransactionId, order.CustomerId, order.TotalPrice);
            
  await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);

  await _paymentProviderClient.Capture(order.TransactionId);            

  await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

  Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
}
```
It is noted that all code in the example is ordinary C#-code. There is no magic going on. What the framework provides in the example is re-trying the function invocation if it fails. 

However, there is more power at your disposal. For instance, how would you solve the issue of the logistics service not being idempotent? That is for each succesfully received request the logistics service will ship all the order's products to the customer. Can we ensure that at-most-one call ever reaches the logistics service? [Curious? You can try to solve the challenge here!](https://github.com/stidsborg/Cleipnir.ResilientFunctions.SagaChallenge/tree/main/Challenge%231)

Also how would you solve the challenge of changing code? I.e. assume a brand enum is added to the order which must be forwarded to the different external services. Can we make code changes directly to the example above affecting both new functions having a brand and old functions now having a brand?
[If you are up for it you can try to solve the challenge here!](https://github.com/stidsborg/Cleipnir.ResilientFunctions.SagaChallenge/tree/main/Challenge%232)

<ins>Message-based:</ins>
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
There is a bit more going on in the example above compared to the previous example. 
However, the flow is actually very similar to RPC-based. For once it is sequential but it is also robust. The flow may crash at any point, be restarted and continue from the point it got to before the crash.

Also, the message broker is just a stand-in - thus not a framework concept - for RabbitMQ, Kafka or some other messaging infrastructure. In a real application the message broker would be replaced with the actual way the application broadcasts a message/event to other services.
Secondly, each resilient function has an associated private event source. When events are received from the outside they can be placed into the relevant resilient function's event source - thereby allowing the function to continue its flow. 

| Did you know? |
| --- |
| The framework allows awaiting events in-memory or suspending the invocation until an event has been appended to the event source |


#### Exception Handling / Rollback logic
The acute reader might have noticed that the previous examples are lacking exception handling. Primarily, this is to keep them simple. The nitty gritty details of communicating with the payment provider, email and logistics services are assumed to exist inside their respective methods. Thus, this is where the relevant exception handling and retry logic resides. 

However, as the code inside a resilient function is ordinary C#-code you are free to implement exception handling as you think fit. Moreover, the framework has constructs for applying exponential or linear backoff strategies around a user specified function (see backoff strategies under scenarios). This provides a simple way to add retry logic to your code. 

## Show me more code
Firstly, the compulsory, ‘*hello world*’-example can be realized as follows:

### Hello-World
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
public static async Task RegisterAndInvoke()
{
  var store = new InMemoryFunctionStore();
        
  var functions = new RFunctions(
    store,
    new Settings(UnhandledExceptionHandler: Console.WriteLine)
  );

  var rAction = functions
    .RegisterActionWithScrapbook<MailAndRecipients, EmailSenderSaga.Scrapbook>(
      "OffersMailSender",
      EmailSenderSaga.Start
    ).Invoke;

  var offerDate = new DateOnly(2022, 1, 1);
  await rAction(
    functionInstanceId: offerDate.ToString(),
    param: new MailAndRecipients(
      new[]
      {
        new EmailAddress("Peter Hansen", "peter@gmail.com"),
        new EmailAddress("Ulla Hansen", "ulla@gmail.com")
      },
      Subject: "Dreaming yourself away?",
      Content: "We have found these great offers for you!"
    )
  );
        
  Console.WriteLine("Offers sent successfully");
}

public static async Task<Return> StartMailSending(MailAndRecipients mailAndRecipients, Scrapbook scrapbook)
{
  var (recipients, subject, content) = mailAndRecipients;
  if (!scrapbook.Initialized)
  {
    //must be first invocation - add all recipients to scrapbook's queue
    foreach (var recipient in recipients)
      scrapbook.RecipientsLeft.Enqueue(recipient);

    scrapbook.Initialized = true;
    await scrapbook.Save();
  }

  using var client = new SmtpClient();
  await client.ConnectAsync("mail.smtpbucket.com", 8025);
        
  while (scrapbook.RecipientsLeft.Any())
  {
    var recipient = scrapbook.RecipientsLeft.Dequeue();
    var message = new MimeMessage();
    message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
    message.From.Add(new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

    message.Subject = subject;
    message.Body = new TextPart(TextFormat.Html) { Text = content };
    await client.SendAsync(message);

    await scrapbook.Save();
  }
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/tree/main/Samples/Sample.ConsoleApp/EmailOffers)
