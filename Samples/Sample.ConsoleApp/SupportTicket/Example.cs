using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.SupportTicket;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<SupportTicketRequest, WorkflowState>(
                functionTypeId: "SupportTicketSaga",
                Saga.AcceptSupportTicket
            );
        
        var messageWriters = registration.MessageWriters;
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is TakeSupportTicket takeSupportTicket && int.Parse(takeSupportTicket.RequestId) == 2)
                await messageWriters.For(takeSupportTicket.Id.ToString()).AppendMessage(
                    new SupportTicketTaken(takeSupportTicket.Id, takeSupportTicket.CustomerSupportAgentEmail, takeSupportTicket.RequestId)
                );
        });

        var request = new SupportTicketRequest(
            SupportTicketId: Guid.NewGuid(),
            CustomerSupportAgents: new[] { "peter@gmail.com", "ole@hotmail.com", "ulla@bing.com" }
        ); 
        
        await registration.Schedule(request.SupportTicketId.ToString(), request);
        
        Console.WriteLine("Press enter to exit");
        Console.ReadLine();
    }
}