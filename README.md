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
```
Install-Package Cleipnir.ResilientFunctions.SqlServer
```

## Elevator Pitch
Still curious - ok awesome - then here comes our elevator pitch example:
```csharp
public static async Task ElevatorPitch(string connectionString)
{
  var store = new SqlServerFunctionStore(connectionString); //simple to use SqlServer as function storage layer 
                                                            //other stores also exist!
  await store.Initialize(); //create table in database
                            //btw the invocation is idempotent!

  var rFunctions = new RFunctions( //this is where you register different resilient function types
    store,
    unhandledExceptionHandler: //framework exceptions are simply to log and handle otherwise - just register a handler
      e => Log.Error(e, "Resilient Function Framework exception occured"),
    crashedCheckFrequency: TimeSpan.FromMinutes(1), // you have the control in deciding the sweet spot 
    postponedCheckFrequency: TimeSpan.FromMinutes(1) // between quick reaction and pressure on the function store
  );

  var registration = rFunctions.Register( //just register a function to make it resilient
    functionTypeId: "HttpGetSaga", //a specific resilient function is identified by type and instance id 
                                   //instance id is provided on invocation
    inner: async Task<Return<string>>(string url) 
      => await HttpClient.GetStringAsync(url) //this is the function you are making resilient!
  ); //btw no need to define a cluster - just register it on multiple nodes to get redundancy!
     //and any crashed invocation of the function type will automatically be picked after this point

  var rFunc = registration.Invoke; //you can also re-invoke (useful for manual handling) an existing function 
                                   //or schedule one for invocation
  const string url = "https://google.com";
  var result = //invoking the function - btw you can F11-debug from here into your registered function 
    await rFunc(functionInstanceId: "google", param: url); 
                                                           
  var responseBody = result.EnsureSuccess(); // you can also check if the invocation failed or was postponed
  Log.Information("Resilient Function getting {Url} completed successfully with {Body}", url, responseBody);
        
  await rFunctions.ShutdownGracefully(); //waits for currently invoking functions to complete before shutdown
                                         //otw just do not await!
}
```

## Show me more code
Firstly, the compulsory, ‘*hello world*’-example can be realized as follows:

### Hello-World
```csharp
var store = new InMemoryFunctionStore();
var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);

var rFunc = functions.Register<string, string>(
  "HelloWorld",
  async param => param.ToUpper()
).Invoke;

var returned = await rFunc(id: "", param: "hello world").EnsureSuccess();
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

  var rAction = functions.Register(
    functionTypeId: "HttpAndDatabaseSaga",
    inner: async (Guid id) =>
    {
      var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
      response.EnsureSuccessStatusCode();
      var content = await response.Content.ReadAsStringAsync();
      await connection.ExecuteAsync("UPDATE Entity SET State=@State WHERE Id=@Id", new {State = content, Id = id});
      return Return.Succeed;
    }).Invoke;

  var id = Guid.NewGuid();
  await rAction(functionInstanceId: id.ToString(), param: id).EnsureSuccess();
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Samples/Sample.ConsoleApp/Simple/SimpleHttpAndDbExample.cs)

### Sending customer emails
Consider a travel agency which wants to send a promotional email to its customers:
```csharp
public static async Task RegisterAndInvoke()
{
  var store = new InMemoryFunctionStore();
  var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);
  var rAction = functions.Register<MailAndRecipients, EmailSenderSaga.Scrapbook>(
    "OffersMailSender", StartMailSending
  ).Invoke;

  var offerDate = new DateOnly(2022, 1, 1);
  var result = await rAction(
    functionInstanceId: offerDate.ToString(),
    param: new MailAndRecipients(
      new[]
        { new EmailAddress("Peter Hansen", "peter@gmail.com"),
          new EmailAddress("Ulla Hansen", "ulla@gmail.com") },
      Subject: "Dreaming yourself away?",
      Content: "We have found these great offers for you!"
    )
  );

  result.EnsureSuccess();
  Console.WriteLine("Offers sent successfully");
}

public static async Task<Return> StartMailSending(MailAndRecipients mailAndRecipients, Scrapbook scrapbook)
{
  var (recipients, subject, content) = mailAndRecipients;
  if (!scrapbook.Initialized)
  {
    //must be first invocation - add all recipients to scrapbook's queue
    foreach (var recipient in recipients)
    {
      scrapbook.RecipientsLeft.Enqueue(recipient);
      scrapbook.Initialized = true;
      await scrapbook.Save();
    }
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

   return Succeed.WithoutValue;
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
rFunc(instanceId, param);
```

Please note it is simple to F11-debug into the registered inner function. The inner function invocation happens inside the first layer of framework code. 

### Ensuring a crashed function completes
When registering a function type the invocation of any crashed function of that type will automatically be restarted by the framework.
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
Network communication is inherently unreliable. Thus, an executing function must handle unsuccessful communication with an external system. 

A common strategy when facing network communication issues is to try again after a short period of time. As an inner function in the framework is ordinary C#-code the retry logic can reside inside the function itself, using a retry-framework such as Polly (https://github.com/App-vNext/Polly). However, if the delay between retries becomes too large it might be more beneficial to persist the function and fetch it again when it becomes eligible for execution again. This saves resources on the running instance as state is moved from memory to persistent storage. The framework supports this out-of-the-box. A function can be postponed simply by returning the intent from the inner function as follows:

```csharp
return Postpone.For(TimeSpan.FromHours(1));
```

#### Back-off strategies:
It is common to increase the delay between retries using some mathematical function in order to put less pressure on the unresponsive system. E.g. linear or exponential backoff strategies. The simplest approach for using such strategies within the framework is to store a retry counter in a scrapbook associated with the function. For instance:

```csharp
public static async Task<Return> InnerFunc(string @param, Scrapbook scrapbook)
{
  var success = await ExternalCall();
  if (!success)
  {
    scrapbook.RetryCount++;
    return Postpone.For(CalculateDelay(scrapbook.RetryCount));
  }
  return Return.Succeed;
}

public class Scrapbook : RScrapbook
{
  public int RetryCount { get; set; }
}
```

### Failing an invocation for manual handling:
It is often infeasible (especially for distributed systems) to take every possible scenario into account when coding. Thus, it is sometimes simpler to flag an issue for human intervention. A function can be failed simply by returning the intent from the inner function as follows:

```csharp
return Fail.WithException(new InvalidOperationException(...));
```

A failed function will not be retried by the framework. However, it is possible to invoke the function again using the function registration’s reinvoke-method. 
```csharp
var registration = rFunctions.Register(functionType, innerFunc);
var reInvokeRFunc = registration.ReInvoke;
reInvokeRFunc(instanceId);
```

Please note that if a registered inner function throws an unhandled exception it is failed by the runtime. 

## Resilient Function Anatomy
### Defintion
A resilient function is simply a wrapped user-specified function (called inner function) with a certain signature. 

The inner function must return either a ```Task<Return>``` or ```Task<Return<T>>``` and accept a user-specified parameter as well as an optional scrapbook-parameter.   

  For instance, concretely, the inner function could have any of the following signatures:
* ```Task<Return<Booking>> ProcessOrder(Order order, Scrapbook scrapbook)```
* ```Task<Return>> ProcessOrder(Order order, Scrapbook scrapbook)```
* ```Task<Return<Booking>> ProcessOrder(Order order)``` 
* ```Task<Return>> ProcessOrder(Order order)```

Which would result in one of the following resilient function signatures:
* ```Task<Result<Booking>> RFunc(string id, Order param)``` 
* ```Task<Result> RAction(string id, Order param)```
* ```Task<Result<Booking>> RFunc(string id, Order param)```
* ```Task<Result> RAction(Order param)```

It is noted that a resilient function returning a non-generic Result is also called a *resilient action* and an inner function returning a non-generic Return-type is  called an *inner action*. 
  
#### Input parameter
The first parameter of all inner function types is the input parameter. It is the idempotent and serializable information which is required for the function to perform its task. 

#### Scrapbook
A scrapbook is a user-specified subtype (of the abstract RScrapbook type) which is used to persist information regarding and during the invocation of the function. The information will be available on re-invocations - e.g. if the function crashes or is postponed. Thus, the scrapbook can be used to avoid blindly restarting the resilient function from scratch after each re-invocation. 
  
Commonly, a scrapbook type is specifically designed for the resilient function at hand. That is, the scrapbook type has properties which make sense for the use-case at hand. Furthermore, a scrapbook must inherit from the abstract RScrapbook framework class and have an empty constructor. 

#### Return-type
An inner function may return either:
* ```Task<Return>```
* ```Task<Return<T>>```

The first corresponds to a void-method whereas the second represents a value returning method. Returning a successful void-result can be accomplished as follows: 
```csharp
async Task<Return> Inner(string param) => Succeed.WithoutValue;
```

Whereas a non-void value can be returned as follows:
```csharp
async Task<Return<string>> Inner(string param) => param.ToUpper();
```

Apart from indicating a successful invocation the Return-type also allows postponing and failing an invocation as follows:
```csharp
async Task<Return<string>> Inner(string param) => Postpone.For(10_000);
```
  
```csharp
async Task<Return<string>> Inner(string param) => Fail.WithException(new TimeoutException());
```  

#### Result
The resilient function created from the inner function returns either Result or Result<T>. The Result-type allows determining if the invocation was successful, postponed or failed. Furthermore, if the invocation was successful then the return value can be extracted in the following way:
```csharp
var result = await rFunc(“hello world”);
var returnValue = result.EnsureSuccess();
```
  
Or using the short-hand:
```csharp
var returnValue = await rFunc(“hello world”).EnsureSuccess();
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
