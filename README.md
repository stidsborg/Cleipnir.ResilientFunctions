# Cleipnir Resilient Functions
**Simply:** Ensuring your invocation completes.

**That is, despite:** failures, restarts, deployments, versioning... 

**Psst you also get:** 
cloud independance, simple debuggability, support for multiple databases, ability to migrate executing functions, testability…  

## Getting Started
A nuget package is coming shortly. However, until then clone our repo to get started: 

```git clone https://github.com/stidsborg/Cleipnir.ResilientFunctions.git```

## Show me the Code
Firstly, the compulsory, ‘hello world’-example can be realized as follows:

### Hello-World
```csharp
var store = new InMemoryFunctionStore();
var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);

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
public static async Task Perform(IDbConnection connection)
{
 var store = new InMemoryFunctionStore();
 var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
 var httpClient = new HttpClient();

 var rAction = functions.Register(
   "SimpleSaga",
   async (Guid id) =>
   {
     var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
     response.EnsureSuccessStatusCode();
     var content = await response.Content.ReadAsStringAsync();
     await connection.ExecuteAsync(
       "UPDATE Entity SET State=@State WHERE Id=@Id", 
       new {State = content, Id = id}
     );
     return Return.Succeed;
   }).Invoke;

 var id = Guid.NewGuid();
 await rFunc(id.ToString(), id).EnsureSuccess();
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/blob/main/Samples/Sample.ConsoleApp/Simple/SimpleHttpAndDbExample.cs)

### Sending customer emails
Consider a travel agency which wants to send a promotional email to its customers:
```csharp
public static async Task RegisterAndInvoke()
{
  var store = new InMemoryFunctionStore();
  var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
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
    message.From.Add(
      new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

    message.Subject = subject;
    message.Body = new TextPart(TextFormat.Html) { Text = content };
    await client.SendAsync(message);

    await scrapbook.Save();
   }

   return Succeed.WithoutValue;
}
```
[Source Code](https://github.com/stidsborg/Cleipnir.ResilientFunctions/tree/main/Samples/Sample.ConsoleApp/EmailOffers)


More documentation to be provided soon...
