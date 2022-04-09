using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.EmailOffers;

public static class Example
{
    public static async Task Perform()
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
}