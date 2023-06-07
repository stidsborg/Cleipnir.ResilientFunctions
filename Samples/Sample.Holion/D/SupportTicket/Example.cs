namespace Sample.Holion.D.SupportTicket;

public static class Example
{
    public static async Task Perform(Flows flows)
    {
        var supportTicketFlows = flows.SupportTicketFlows;
        
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is TakeSupportTicket takeSupportTicket && int.Parse(takeSupportTicket.RequestId) == 2)
                await supportTicketFlows.EventSourceWriter(takeSupportTicket.Id.ToString()).AppendEvent(
                    new SupportTicketTaken(takeSupportTicket.Id, takeSupportTicket.CustomerSupportAgentEmail, takeSupportTicket.RequestId)
                );
        });

        var request = new SupportTicketRequest(
            SupportTicketId: Guid.NewGuid(),
            CustomerSupportAgents: new[] { "peter@gmail.com", "ole@hotmail.com", "ulla@bing.com" }
        ); 
        
        await supportTicketFlows.Run(request.SupportTicketId.ToString(), request);
        
        Console.WriteLine("Press enter to exit");
        Console.ReadLine();
    }
}