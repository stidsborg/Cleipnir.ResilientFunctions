using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using MailKit.Net.Smtp;
using MimeKit;
using MimeKit.Text;

namespace ConsoleApp.EmailOffers;

public static class EmailSenderSaga
{
    public static async Task Start(MailAndRecipients mailAndRecipients, Workflow workflow)
    {
        var atRecipient = await workflow.Effect.CreateOrGet(0, value: 0);
        var (recipients, subject, content) = mailAndRecipients;

        using var client = new SmtpClient();
        await client.ConnectAsync("mail.smtpbucket.com", 8025);
        
        for (; atRecipient < mailAndRecipients.Recipients.Count; atRecipient++)
        {
            var recipient = recipients[atRecipient];
            var message = new MimeMessage();
            message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
            message.From.Add(new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

            message.Subject = subject;
            message.Body = new TextPart(TextFormat.Html) { Text = content };
            await client.SendAsync(message);

            await workflow.Effect.Upsert(0, atRecipient);
        }
    }
}

public record EmailAddress(string Name, string Address);
public record MailAndRecipients(
    List<EmailAddress> Recipients,
    string Subject,
    string Content
);