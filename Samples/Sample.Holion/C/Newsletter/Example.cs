namespace Sample.Holion.C.Newsletter;

public static class Example
{
    public static async Task Perform(Flows flows)
    {
        var newsletterFlows = flows.NewsletterFlows;
        var publishDate = new DateOnly(2022, 1, 1);
        await newsletterFlows.Run(
            instanceId: publishDate.ToString(),
            new MailAndRecipients(
                new List<EmailAddress>
                {
                    new("Peter Hansen", "peter@gmail.com"),
                    new("Ulla Hansen", "ulla@gmail.com")
                },
                Subject: "Event Sourcing Pros & Cons",
                Content: "Did you ever wonder what the pros..."
            )
        );
        
        Console.WriteLine("Offers sent successfully");
    }
}