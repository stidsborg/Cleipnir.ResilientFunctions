﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.SupportTicket;

public class Saga
{
    public static async Task AcceptSupportTicket(SupportTicketRequest request, Workflow workflow)
    {
        var messages = workflow.Messages;
        
        var agents = request.CustomerSupportAgents.Length;
        for (var i = 0; ; i++)
        {
            await workflow.Effect.Capture("SendTakeSupportTicketRequest", async () =>
            {
                var customerSupportAgentEmail = request.CustomerSupportAgents[i % agents];
                await MessageBroker.Send(
                    new TakeSupportTicket(request.SupportTicketId, customerSupportAgentEmail, RequestId: i.ToString())
                );
            });
            
            var supportTicketTakenOption = await messages
                .TakeUntilTimeout($"TimeoutId{i}", expiresIn: TimeSpan.FromMinutes(15))
                .OfType<SupportTicketTaken>()
                .Where(t => int.Parse(t.RequestId) == i)
                .FirstOrNone(TimeSpan.Zero);

            if (supportTicketTakenOption.HasValue)
                return;
        }
    }
}