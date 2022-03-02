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
    public static async Task<Return> Start(MailAndRecipients mailAndRecipients, Scrapbook scrapbook)
    {
        var (recipients, subject, content) = mailAndRecipients;
        if (scrapbook.Initialized && scrapbook.RecipientsLeft.Count == 0) return Succeed.WithoutValue;
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

        return Succeed.WithoutValue;
    }

    public class Scrapbook : RScrapbook
    {
        public Queue<EmailAddress> RecipientsLeft { get; set; } = new();
        public bool Initialized { get; set; }
    }
}

public record EmailAddress(string Name, string Address);
public record MailAndRecipients(
    IEnumerable<EmailAddress> Recipients,
    string Subject,
    string Content
);