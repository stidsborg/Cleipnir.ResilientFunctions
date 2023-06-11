using Cleipnir.ResilientFunctions.Domain;
using MailKit.Net.Smtp;
using MimeKit;
using MimeKit.Text;

namespace Sample.Holion.C.Newsletter;

public class NewsletterFlow : Flow<MailAndRecipients, NewsletterFlow.NewsletterScrapbook>
{
    public override async Task Run(MailAndRecipients mailAndRecipients)
    {
        var (recipients, subject, content) = mailAndRecipients;

        using var client = new SmtpClient();
        await client.ConnectAsync("mail.smtpbucket.com", 8025);

        throw new NotImplementedException();
    }

    private async Task SendNewsletter(SmtpClient client, EmailAddress recipient, string subject, string content)
    {
        var message = new MimeMessage();
        message.To.Add(new MailboxAddress(recipient.Name, recipient.Address));
        message.From.Add(new MailboxAddress("Cleipnir.NET", "newsletter@cleipnir.net"));

        message.Subject = subject;
        message.Body = new TextPart(TextFormat.Html) { Text = content };
        await client.SendAsync(message);
    }

    public class NewsletterScrapbook : RScrapbook
    {
        
    }
}

public record EmailAddress(string Name, string Address);
public record MailAndRecipients(
    List<EmailAddress> Recipients,
    string Subject,
    string Content
);