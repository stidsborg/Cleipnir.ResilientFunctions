﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Timeout = Cleipnir.ResilientFunctions.Domain.Events.Timeout;

namespace ConsoleApp.SupportTicket;

public class Saga
{
    public static async Task AcceptSupportTicket(SupportTicketRequest request, Scrapbook scrapbook, Context context)
    {
        var eventSource = await context.EventSource;
        
        var agents = request.CustomerSupportAgents.Length;
        while (true)
        {
            if (!TimeoutOrResponseForTryReceived(eventSource, scrapbook.Try))
            {
                var customerSupportAgentEmail = request.CustomerSupportAgents[scrapbook.Try % agents];
                await MessageBroker.Send(new TakeSupportTicket(request.SupportTicketId, customerSupportAgentEmail, RequestId: scrapbook.Try.ToString()));
                await eventSource.TimeoutProvider.RegisterTimeout(timeoutId: scrapbook.Try.ToString(), expiresIn: TimeSpan.FromSeconds(5));                
            }
            
            var either = await eventSource
                .OfTypes<SupportTicketTaken, Timeout>()
                .Where(e => e.Match(stt => int.Parse(stt.RequestId), t => int.Parse(t.TimeoutId)) == scrapbook.Try)
                .SuspendUntilNext();

            if (either.HasFirst)
                return;

            scrapbook.Try++;
            await scrapbook.Save();
        }
    }

    public class Scrapbook : RScrapbook
    {
        public int Try { get; set; }
    }

    private static bool TimeoutOrResponseForTryReceived(EventSource eventSource, int @try)
    {
        return eventSource
            .OfTypes<SupportTicketTaken, Timeout>()
            .Where(e => e.Match(stt => int.Parse(stt.RequestId), t => int.Parse(t.TimeoutId)) == @try)
            .PullExisting()
            .Any();
    }
}