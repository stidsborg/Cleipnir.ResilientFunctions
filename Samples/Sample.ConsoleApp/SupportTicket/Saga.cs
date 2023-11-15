using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.SupportTicket;

public class Saga
{
    public static async Task AcceptSupportTicket(SupportTicketRequest request, RScrapbook scrapbook, Context context)
    {
        var eventSource = context.EventSource;
        
        var agents = request.CustomerSupportAgents.Length;
        for (var i = 0; ; i++)
        {
            await scrapbook.DoAtLeastOnce("SendTakeSupportTicketRequest", async () =>
            {
                var customerSupportAgentEmail = request.CustomerSupportAgents[i % agents];
                await MessageBroker.Send(
                    new TakeSupportTicket(request.SupportTicketId, customerSupportAgentEmail, RequestId: i.ToString())
                );
            });
            
            var supportTicketTakenOption = await eventSource
                .OfType<SupportTicketTaken>()
                .Where(t => int.Parse(t.RequestId) == i)
                .TakeUntilTimeout($"TimeoutId{i}", expiresIn: TimeSpan.FromMinutes(15))
                .SuspendUntilFirstOrNone();

            if (supportTicketTakenOption.HasValue)
                return;
            
            await scrapbook.Save();
        }
    }
}