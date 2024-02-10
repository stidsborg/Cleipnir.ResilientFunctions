using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using MailKit.Net.Smtp;
using MimeKit;
using MimeKit.Text;

namespace ConsoleApp.EmailOffers;

public static class EmailSenderSaga
{
    public static async Task Start(MailAndRecipients mailAndRecipients, State state)
    {
        var (recipients, subject, content) = mailAndRecipients;

        using var client = new SmtpClient();
        await client.ConnectAsync("mail.smtpbucket.com", 8025);
        
        for (var atRecipient = state.AtRecipient; atRecipient < mailAndRecipients.Recipients.Count; atRecipient++)
        {
            var recipient = recipients[atRecipient];
            var message = new MimeMessage();
            message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
            message.From.Add(new MailboxAddress("The Travel Agency", "offers@thetravelagency.co.uk"));

            message.Subject = subject;
            message.Body = new TextPart(TextFormat.Html) { Text = content };
            await client.SendAsync(message);

            state.AtRecipient = atRecipient;
            await state.Save();
        }
    }

    public class State : WorkflowState
    {
        public int AtRecipient { get; set; }
    }
}

public record EmailAddress(string Name, string Address);
public record MailAndRecipients(
    List<EmailAddress> Recipients,
    string Subject,
    string Content
);