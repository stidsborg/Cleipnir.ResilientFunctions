﻿using System;
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
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<SupportTicketRequest, Saga.Scrapbook>(
                functionTypeId: "SupportTicketSaga",
                Saga.AcceptSupportTicket
            );
        
        var eventSourceWriters = registration.EventSourceWriters;
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is TakeSupportTicket takeSupportTicket && int.Parse(takeSupportTicket.RequestId) == 2)
                await eventSourceWriters.For(takeSupportTicket.Id.ToString()).AppendEvent(
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