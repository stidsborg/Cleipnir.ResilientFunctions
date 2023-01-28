# Cleipnir's Resilient Functions
**"Simply providing a way to ensure your code gets run - until you say it is done"**

Resilient Functions is a simple and intuitive .NET framework for managing the execution of functions which must complete in their entirety despite: failures, restarts, deployments, versioning etc. 

It automatically retries a function invocation until it completes potentially across process restarts and physical nodes. 

The framework also supports postponing/suspending invocations or failing invocations for manually handling. Furthermore, versioning is natively supported.

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances.

Crucially, all this allows the **saga pattern** to be implemented in a simple yet powerful way. 

Out-of-the-box you also get:
* synchronized invocation across multiple process instances
* cloud independance & support for multiple databases
* simple debuggability
* ability to migrate non-completed functions
* simple testability

If you like a slide deck can be found [here](https://github.com/stidsborg/Cleipnir.ResilientFunctions.Documentation/raw/main/Presentation.pdf)  

| What it is not? |
| --- |
| Resilient Functions is not a message-broker based solution. Meaning, it does not need a message-broker to operate. Also there is no event replay or similar occuring when it retries a function invocation. <br /><br />In short, when a function is re-invoked the latest state is passed to the function by the framework and nothing else.<br /><br />This is in line with the framework’s basic tenet of simplicity and understandability. Thus, making it as simple as possible to ensure a piece of logic is executed while at the same time ensuring the execution is easy to reason about. |

## Sections
* [Getting Started](#getting-started)
* [Elevator Pitch](#elevator-pitch)
* [Introduction](#introduction)
* [Show me more code](#show-me-more-code)
  * [Hello World](#hello-world) 
  * [HTTP-call & Database](#http-call--database)
  * [Sending customer emails](#sending-customer-emails)
  * [ASP.NET Core Integration](#aspnet-core-integration) 
* [Simple Scenarios](#simple-scenarios)
  * [Invoking a resilient function](#invoking-a-resilient-function) 
  * [Ensuring a crashed function completes](#ensuring-a-crashed-function-completes) 
  * [Storing rainy-day state](#storing-rainy-day-state) 
  * [Postponing an invocation](#postponing-an-invocation) 
  * [Back-off strategies](#back-off-strategies) 
  * [Failing an invocation for manual handling](#failing-an-invocation-for-manual-handling) 
* [Resilient Function Anatomy](#resilient-function-anatomy)

## Getting Started
```powershell
Install-Package Cleipnir.ResilientFunctions.SqlServer
```
or
```powershell
Install-Package Cleipnir.ResilientFunctions.PostgreSQL
```
and optionally
```powershell
Install-Package Cleipnir.ResilientFunctions.AspNetCore
```

## Elevator Pitch
Still curious - ok awesome - then here comes our elevator pitch example:
```csharp
var store = new SqlServerFunctionStore(connectionString); //simple to use SqlServer as function storage layer - other stores also exist!
await store.Initialize(); //create table in database - btw the invocation is idempotent!

var rFunctions = new RFunctions( //this is where you register different resilient function types
  store,
  new Settings(
    UnhandledExceptionHandler: //framework exceptions are simply to log and handle otherwise - just register a handler
      e => Log.Error(e, "Resilient Function Framework exception occured"),
    CrashedCheckFrequency: TimeSpan.FromMinutes(1), // you are in control deciding the sweet spot 
    PostponedCheckFrequency: TimeSpan.FromMinutes(1) // between quick reaction and pressure on the function store
    )
  );

  var registration = rFunctions.RegisterFunc( //making a function resilient is simply a matter of registering it
    functionTypeId: "HttpGetSaga", //a specific resilient function is identified by type and instance id - instance id is provided on invocation
    inner: async Task<string>(string url) => await HttpClient.GetStringAsync(url) //this is the function you are making resilient!
  ); //btw no need to define a cluster - just register it on multiple nodes to get redundancy!
     //also any crashed invocation of the function type will automatically be picked after this point

  var rFunc = registration.Invoke; //you can also re-invoke (useful for manual handling) an existing function or schedule one for invocation
  const string url = "https://google.com";
  var responseBody = await rFunc(functionInstanceId: "google", param: url); //invoking the function - btw you can F11-debug from here into your registered function
  Log.Information("Resilient Function getting {Url} completed successfully with body: {Body}", url, responseBody);
        
  await rFunctions.ShutdownGracefully(); //waits for currently invoking functions to complete before shutdown - otw just do not await!
```

## Introduction
Almost certainly there exists some code/method/business process within your application which must be executed in its entirety in order to avoid inconsistencies. 

A prime example is that of an order processing flow where every contained step must be executed. If the flow is only partly executed a situation can arise where the ordered product might be shipped but funds never deducted from the customer’s credit card. 

Unfortunately, the operating system process executing our code might crash at any time be that unexpectedly due to underlying hardware failures or simply because of a deployment of a new version of our application. Per default nothing keeps track of such issues. They can therefore be hard to track down and fix.

Cleipnir’s Resilient Functions framework helps you manage all this complexity while automatically retrying your function invocation. It is exceptionally simple to set up and get started, designed to be a slim and intuitive abstraction (no-magic). In short, it provides a way to ensure your code gets executed - until you say it is done. 

| Two Transactions’ Problem (pun intended) |
| --- |
| The problem addressed by the framework boils down to a fundamental limitation of distributed computing and the ephemerality of the executing state of our application. <br /><br />Imagine you have two distinct database transactions (which cannot be turned into one transaction) and you are tasked with writing a method ensuring both transactions are committed. Let’s assume you come up with the following simple solution:<br /><br />```void CommitBoth(Transaction t1, Transaction t2) { t1.Commit(); t2.Commit(); }```<br /><br />A PR review might suggest adding exception handling and retry logic. However, the other fundamental reality that our application exists in makes this a futile effort. An application might suddenly at any time crash. This might be due to hardware failures or simply deployment of a new version of our software. <br /><br />In the end no matter the effort we cannot write a method which ensures that both or no transactions are committed when we lose track of where we got to in the execution of our application. <br /><br />The Resilient Functions framework tackles the problem by keeping track of the “CommitBoth”-method invocation and retries it until it eventually succeeds.|

**Recipe: Making your code resilient**
1. Get the nuget package(s) 
2. Setup the framework at application startup 
```csharp
var store = new SqlServerFunctionStore(ConnectionString);
await store.Initialize();
var rFunctions = new RFunctions(store);
```
3. Register your function with the framework and use the returned delegate in your code-base
```csharp
var processOrderRFunc = rFunctions.RegisterAction<Order>(
  functionTypeId,
  ProcessOrder
).Invoke;
await processOrderRFunc(functionInstanceId, parameter); //from here onwards the framework ensures that the function invocation completes…
```

This is all the setup that is required in order to get started. Just provide a connection string, register the function you want to make resilient and use the returned function in your code base. There is no cluster management - thus, to get fail-over and high-availability simply register the function on multiple running instances. 

### Business Processes needing Resiliency
Business processes which must be executed in their entirety are surprisingly common in today's microservice architectures. The situation typically arises when a process needs to communicate with one or more microservices and potentially its own database in order to fulfill its task. If the operating system process executing the flow crashes while executing it, the system as a whole will be in an inconsistent state, where parts of the system reflect the operations and others do not. Per default nothing will rectify or notify you of this situation. 

| Monolithic Approach |
| --- |
| The problem described above stands in contrast to how business processes were handled before the rise of microservices. In a monolithic architecture where an application only has one database and there is no communication with external dependencies, the problem can easily be solved by using ACID transactions. <br />Simply ensure that:<br />A. When a request is received a transaction is started. <br />B. All side effects performed when handling the request are wrapped inside the transaction.<br />C. Immediately before completing the processing of the request the transaction is committed. <br /><br />However, in a microservice architecture this is not feasible. Thus, another mechanism is required for ensuring that all side effects are committed.|

A simple ubiquitous example of a business process needing resiliency is that of an order processing flow. A simplified version could involve the following steps:
1. Reserve funds from the customer’s credit card
2. Ship ordered products to the customer
3. Send confirmation email to the customer
4. Capture funds from the customer’s credit card

Or equivalently in code:
```csharp
public static async Task ProcessOrder(Order order)
{
  var reservationId = await PaymentProvider.ReserveFunds(
    order.CustomerId,
    amount: order.Products.Sum(p => p.Price)
  );
  await LogisticsService.ShipProducts(order.Products);
  await EmailService.SendConfirmationEmail(order.CustomerEmail);
  await PaymentProvider.CaptureFunds(reservationId);
}
```

When implementing this business flow without framework assistance it is not guaranteed that the flow will complete. In fact, as the process executing it may crash at any time during the flow, we might end up in a situation in which the products have been sent to the customer but without receiving payment for them.

### Diving a bit deeper
When retrying a method invocation after a crash it is not always meaningful to naively execute the code in the same way as previously.  

In a distributed system the endpoints we invoke can have different behaviors. It might be fine to invoke an endpoint multiple times (so called idempotent end-point) or it may not be the case (at-most-once endpoint). Furthermore, when invoking an idempotent endpoint we might need to supply a request id, which allows the endpoint on the other side to detect if it is a new request or if it has already been handled previously. The Resilient Functions’ framework addresses all these concerns in a simple yet powerful way. 

#### Introducing the Scrapbook-type 
At a high-level a scrapbook is a user-defined type which contains useful information to be used when retrying a method invocation. Any registered resilient function may accept a scrapbook-parameter. A scrapbook is automatically created and implicitly persisted by the framework.

Let us imagine that a new idempotent version of the payment provider’s endpoint in the previously presented example was published. In turn this would change the internals of the method in the following way:   

```csharp
public static async Task ProcessOrder(Order order)
{
  var requestId = Guid.NewGuid();
  await PaymentProvider.ReserveFunds(
    requestId,
    order.CustomerId,
    amount: order.Products.Sum(p => p.Price)
  );
  await LogisticsService.ShipProducts(order.Products);
  await EmailService.SendConfirmationEmail(order.CustomerEmail);
  await PaymentProvider.CaptureFunds(requestId);
}
```

As shown, it is now up to the sender to generate and supply a request id to the payment provider allowing it to determine if a request has been handled before. However, the current version does not handle process crashes and subsequent retries correctly. Each re-invocation would result in a new request id being created and in turn a new reservation and potentially captured payment. 

In order to rectify this issue a scrapbook can be used, as follows:  

```csharp
public static async Task ProcessOrder(Order order, Scrapbook scrapbook)
{
  if (scrapbook.RequestId == null) 
  {
    scrapbook.RequestId = Guid.NewId();
    await scrapbook.Save();
  }

  await PaymentProvider.ReserveFunds(
    scrapbook.RequestId,
    order.CustomerId,
    amount: order.Products.Sum(p => p.Price)
  );
  await LogisticsService.ShipProducts(order.Products);
  await EmailService.SendConfirmationEmail(order.CustomerEmail);
  await PaymentProvider.CaptureFunds(scrapbook.RequestId);
}

public class Scrapbook : RScrapbook
{
    public Guid RequestId { get; set; }
}
```

Using the scrapbook we ensure that all re-invocations will use the same request id and subsequently that the payment provider will be able to recognize if it is the same reserve or capture request. 

**At-most-once semantics:**
At the other end of the spectrum of end-point “toughness” are end-points which are not idempotent and which perform side-effects. They pose a major challenge against naively retrying a business process after a crash.  

Imagine that the Logistics Service’s endpoint in the “processing order”-example is non-idempotent. That is each request to the endpoint will result in the specified products being shipped to the customer. If it is invoked multiple times then the customer will receive the products multiple times.

A simple solution can be attained by extending the scrapbook. In this way we can ensure that the “ShipProducts”-endpoint is invoked at-most-once and flag the invocation for manual handling if a crash occurs while invoking the “ShipProducts”-endpoint. 

```csharp
public static async Task ProcessOrder(Order order, Scrapbook scrapbook)
{
  if (scrapbook.RequestId == null) 
  {
    scrapbook.RequestId = Guid.NewId();
    await scrapbook.Save();
  }
  if (scrapbook.ShipProductsCallCompleted == false)
    throw new InvalidOperationException("ShipProducts did not complete previously");

  await PaymentProvider.ReserveFunds(
    scrapbook.RequestId,
    order.CustomerId,
    amount: order.Products.Sum(p => p.Price)
  );
  if (scrapbook.ShipProductsCallCompleted != true) 
  { 
    scrapbook.ShipProductsCallCompleted = false;
    await scrapbook.Save();
    await LogisticsService.ShipProducts(order.Products);
    scrapbook.ShipProductsCallCompleted = true;
    await scrapbook.Save();
  }
  await EmailService.SendConfirmationEmail(order.CustomerEmail);
  await PaymentProvider.CaptureFunds(scrapbook.RequestId);
}

public class Scrapbook : RScrapbook
{
    public Guid? RequestId { get; set; }
    public bool? ShipProductsCallCompleted { get; set; }
}
```

A thrown unhandled exception - as with the InvalidOperationException above - will result in the function invocation for that specific order to transition to a failed state. In this state the framework will not try to re-invoke the function automatically. Instead, this state is meant for the case where manual handling is required for the issue to be rectified. Assuming it has been determined that it is safe for the invocation to be retried then the function can be invoked again as follows:

```csharp
var reinvokeProcessOrder = rFunctions.RegisterAction<Order>(
  functionTypeId,
  ProcessOrder
).ReInvoke;

await reinvokeProcessOrder(
  functionInstanceId, 
  parameter, 
  expectedStatuses: new[] {Status.Failed},
  scrapbookUpdater: s => s.ShipProductsCallCompleted = null //resetting flag
); //from here onwards the framework ensures that the function invocation completes…
```

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

### ASP.NET Core Integration
To simplify integration with ASP.NET Core projects the companying nuget package can be used, which automatically registers and initializes the framework within your ASP.NET application. 

```powershell
Install-Package Cleipnir.ResilientFunctions.AspNetCore
```

Further, the package can be configured to postpone shutdown until all executing resilient functions have completed - so called graceful shutdown. 
Using this functionality within your application is simply a matter of adding the following (or similar) snippet to your code-base:

```csharp
builder.Services.AddRFunctionsService(
  store: sp => new PostgreSqlFunctionStore(connectionString),
  settings: sp => 
    new Settings(
      UnhandledExceptionHandler: e => sp.GetRequiredService<ILogger<RFunctions>>().LogError(e, "RFunctions thrown unhandled exception")
    ), 
    gracefulShutdown: true
);
```

The AddRFunctionsService-method has a couple of overloads, thus, allowing several use-cases to be catered for.

#### Registering Functions:
In order for a crashed or postponed resilient function to be invokable within the framework the resilient function’s type must first be registered with a RFunctions instance. 

Using dependency injection may inadvertently delay this registration if the registration is only performed when a type is first resolved. In order to cater for this the framework provides two interfaces which the ASP.NET Core Service will use to ensure that functions are registered on startup. 
* ```IRegisterRFunc```
* ```IRegisterRFuncOnInstantiation```

## Simple Scenarios
It is simple to start using the framework and reap the rewards. In this chapter common and simple scenarios and their solutions are presented to help you get started.

### Invoking a resilient function
After a resilient function has been registered invoking it is simply a matter of invoking the registration’s Invoke-delegate. This can be accomplished as follows:
```csharp
var registration = rFunctions.Register(functionType, innerFunc);
var rFunc = registration.Invoke;
await rFunc(instanceId, param);
```

Please note it is simple to F11-debug into the registered inner function. The inner function invocation happens inside the first or second layer of framework code. 

### Ensuring a crashed function completes
When registering a function type the invocation of any crashed or postponed (eligible for execution) function of that type will automatically be restarted by the framework.
```csharp
var registration = rFunctions.Register(functionType, innerFunc);
```

Furthermore, when the same function type has been registered on multiple RFunctions-instances, the framework automatically balances the load among the different instances while ensuring that a crashed function is only invoked on a single instance. 

### Storing rainy-day state
It is inevitable that a resilient function invocation does not always follow the sunshine path. As such it is often beneficial to have state about previous invocations when invoking a resilient function again. 

The framework has built-in support for a so-called “scrapbook”. A scrapbook is a user-defined type - which inherits from the abstract RScrapbook-type - and as such may be freely designed for the use-case at hand. A scrapbook is an optional second parameter for any registered inner function. 

The framework automatically ensures the scrapbook is stored after the function invocation returns - be that successfully or not. Furthermore, a scrapbook can also be manually stored on the fly using its Save-method. 

See the prior “Sending customer emails”-code for a good example. 

### Postponing an invocation
Network communication is inherently unreliable. Thus, an executing function must be able to handle unsuccessful communication with an external system. 

A common strategy when facing network communication issues is to try again after a short period of time. As an inner function in the framework is ordinary C#-code the retry logic can reside inside the function itself, using a retry-framework such as Polly (https://github.com/App-vNext/Polly). 
However, if the delay between retries becomes too large it might be more beneficial to persist the function and fetch it again when it becomes eligible for execution again. 
This saves resources on the running instance as state is moved from memory to persistent storage.
The framework supports this out-of-the-box. 
A function can be postponed simply by returning the intent from the inner function as follows:

```csharp
return Postpone.For(TimeSpan.FromHours(1));
```

#### Back-off strategies:
It is common to increase the delay between retries using some mathematical function in order to put less pressure on the unresponsive system. E.g. linear or exponential backoff strategies. The framework supports both strategies using function composition. For instance, exponential backoff error-handling can be added to an inner function as follows:

```csharp
var rFuncRegistration = rFunctions.RegisterFunc(
  functionType,
  OnFailure.BackoffExponentially<string, BackoffScrapbook, string>(
    Inner,
    firstDelay: TimeSpan.FromMilliseconds(1000),
    factor: 2,
    maxRetries: 3,
    onException: LogError
  )
);
```

### Failing an invocation for manual handling:
It is often infeasible (especially for distributed systems) to take every possible scenario into account when coding. Thus, it is sometimes simpler to flag an issue for human intervention. A function can be failed simply by throwing an unhandled exception or by explicitly returning a Fail-instance from the inner function:

```csharp
return Fail.WithException(new InvalidOperationException(...));
```

A failed function will not be retried by the framework. However, it is possible to explicitly invoke the function again using the function registration’s reinvoke-method. 
```csharp
var registration = rFunctions.Register(functionType, innerFunc);
var reInvokeRFunc = registration.ReInvoke;
reInvokeRFunc(instanceId, new[] {Status.Failed});
```

## Resilient Function Anatomy
### Defintion
A resilient function is simply a wrapped user-specified function (called inner function), which accepts either one or two parameters and optionally returns a value.    
  
#### Input parameter
The first parameter of any inner function is the input parameter. It is the constant and serializable information which is required for the function to perform its designated task. 

#### Scrapbook
A scrapbook is a user-specified subtype (of the abstract RScrapbook type) which is used to persist information regarding and during the invocation of the function. The information will be available on re-invocations - e.g. if the function crashes or is postponed. Thus, the scrapbook can be used to avoid blindly restarting the resilient function from scratch after each re-invocation. 
  
Commonly, a scrapbook type is specifically designed for the resilient function at hand. That is, the scrapbook type has properties which make sense for the use-case at hand. Furthermore, a scrapbook must inherit from the abstract RScrapbook framework class and have an empty constructor. 

#### Return-type
An inner function may optionally return a value and in either case may explicitly return the outcome of the invocation (e.g. if the invocation should be postponed).

Thus, returning a successful void-result can be accomplished as follows: 
```csharp
void Inner(string param) => {...}
```

Or in the async case:
```csharp
Task Inner(string param) => {...}
```

Whereas a non-void value can be returned as follows:
```csharp
string Inner(string param) => param.ToUpper();
```

Or (again) in the async case:
```csharp
async Task<string> Inner(string param) => param.ToUpper();
```

Apart from indicating a successful invocation the Return-type also allows postponing and failing an invocation as follows:
```csharp
Return<string> Inner(string param) => Postpone.For(10_000);
```
  
```csharp
async Task<Return<string>> Inner(string param) => Fail.WithException(new TimeoutException());
```  

### Identification
A resilient function is uniquely identified by two strings:
* Function Type Id
* Function Instance Id

We note that a *‘resilient function instance’* refers to a specific function with some unique id.

### Registration
In order to make a function *resilient* the function must be registered with the framework. This is done by constructing (or using an already constructed) RFunctions instance and invoking its Register-method. 

```csharp
var registration = rFunctions.Register(
  functionTypeId,
  innerFunc
);
```

The Register-method returns a registration object allowing the previously specified inner function to - by the framework - be:
* invoked
* scheduled for invocation
* re-invoked
* scheduled for re-invocation 
  
Furthermore, registering an inner function enables the framework to re-invoke postponed or crashed functions with the provided function type identifier. 

### Behavior / Semantics
#### Resilient Function Result Cache
When a resilient function invocation succeeds the result is persisted in the associated function store. If a completed function is invoked after the fact the persisted result is returned and used without actually re-invoking the inner function. 

#### Multiple Invocations vs Re-Invocations
There is a distinct difference between multiple invocations of the same resilient function and a re-invocation of a resilient function. 

A somewhat artificial example of invoking the same function multiple times is the following: 
```csharp
var rFunc = rFunctions.Register(...).Invoke;
var task1 = rFunc(id, param);
var task2 = rFunc(id, param);
```
  
The second invocation will not result in the associated inner function being invoked. Instead, the framework detects that the inner function is already invoking and will wait for the previous function invocation to complete before simply returning the result of the previous invocation. 
Despite the example being artificial the situation is more realistic in an actual distributed system where the invocation might concurrently be started inside multiple nodes. In this situation the framework has been designed to avoid invoking the same resilient function multiple times. Instead, the last invocation will simply wait for the result and return it. 

A re-invocation on the other hand is an explicit action performed on an existing resilient function. It is useful for manual intervention. For instance, if a resilient function has failed, then it can be invoked again: 

```csharp  
var reinvokeRFunc = rFunctions.Register(...).ReInvoke;
var result = await reinvokeRFunc(functionInstanceId, expectedStatuses: new[] { Status.Failed });
```

#### Resilient Function Synchronization & Check Frequency
The framework makes a best effort attempt at ensuring that only one resilient function instance is invoked across the cluster at any time. Please note that a resilient function instance is defined by both a function type id and function instance id. Thus, resilient functions with the same type id but different instance id are invoked concurrently by the framework. 

The synchronization mechanism uses a simple counter (called SignOfLife) which is updated as long as the resilient function is invoking. When the counter is not updated in a predefined period of time (called check frequency) the invocation is restarted on another framework instance. The SignOfLife update frequency can be specified when constructing an RFunctions-instance. A high frequency means the function invocation is restarted faster. However, it puts more strain on the system as a whole. As the optimal frequency is affected by the concrete use case and the system infrastructure, the frequency can be specified when constructing a RFunctions-instance. E.g.

```csharp  
var rFunctions = new RFunctions(
  store,
  crashedCheckFrequency: TimeSpan.FromSeconds(60),
  postponedCheckFrequency: TimeSpan.FromSeconds(10)
);
```

#### Crashed vs Failed
Despite the similarity between the two words they have distinct meanings within the framework.
* A crashed function is a function which is no longer emitting a heartbeat
* A failed function is a function which explicitly returned a failed Return-instance

Typically, a function crash occurs on deployments where a process executing a resilient function is taken down. If the process shutdown is not delayed until the framework has been allowed to perform a graceful shutdown, then the function crashes. 

Contrarily, a failed function has been allowed to complete its invocation but is in an erroneous state. This can occur explicitly by returning a Fail-instance or by throwing an unhandled exception. 
  
---
More documentation to be provided soon...
