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