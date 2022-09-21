using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.EmailOffers;

namespace ConsoleApp.Middleware;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
            store,
            new Settings(UnhandledExceptionHandler: Console.WriteLine)
        );

        var rAction = functions
            .RegisterAction<MailAndRecipients, EmailSenderSaga.Scrapbook>(
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

    private class Middleware : IMiddleware
    {
        public Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
            TParam param, 
            TScrapbook scrapbook, 
            Context context, 
            Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
        ) where TParam : notnull where TScrapbook : RScrapbook, new()
        {
            var correlationId = scrapbook.StateDictionary["CorrelationId"];
            Console.WriteLine($"Correlation id was: {correlationId}");
            return next(param, scrapbook, context);
        }
    }
}