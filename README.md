# Cleipnir's Resilient Functions

Resilient Functions is a .NET framework realizing the saga-pattern using .NET funcs / actions. 

By registering a function with the framework, it will ensure that the function invocation completes despite: failures, restarts, deployments, data migrations etc. 

The framework also supports failing invocations for manually handling and facilitates data migrations. 

It requires a minimal amount of setup to get started and seamlessly scales with multiple running instances. 

*Psst* out-of-the-box you also get:
* synchronized invocation across multiple process instances
* cloud independance & support for multiple databases
* simple debuggability
* ability to migrate non-completed functions
* simple testability 

## Getting Started
```powershell
Install-Package Cleipnir.ResilientFunctions.SqlServer
```
or
```powershell
Install-Package Cleipnir.ResilientFunctions.PostgreSQL
```

## Elevator Pitch
Still curious - ok awesome - then here comes our elevator pitch example:
```csharp
var store = new SqlServerFunctionStore(connectionString); //simple to use SqlServer as function storage layer - other stores also exist!
await store.Initialize(); //create table in database - btw the invocation is idempotent!

var rFunctions = new RFunctions( //this is where you register different resilient function types
  store,
  unhandledExceptionHandler: //framework exceptions are simply to log and handle otherwise - just register a handler
    e => Log.Error(e, "Resilient Function Framework exception occured"),
  crashedCheckFrequency: TimeSpan.FromMinutes(1), // you are in control deciding the sweet spot 
  postponedCheckFrequency: TimeSpan.FromMinutes(1) // between quick reaction and pressure on the function store
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
  var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);
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
    unhandledExceptionHandler: Console.WriteLine
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
To simplify integration with ASP.NET Core projects the companying nuget package can be used, which automatically registers and initializes the framework within your ASP.NET application. Further, the package can be configured to postpone shutdown until all executing resilient functions have completed - so called graceful shutdown. 

Using this functionality within your application is simply a matter of adding the following (or similar) snippet to your code-base:

```csharp
builder.Services.AddRFunctionsService(
  store,
  unhandledExceptionHandler,
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
