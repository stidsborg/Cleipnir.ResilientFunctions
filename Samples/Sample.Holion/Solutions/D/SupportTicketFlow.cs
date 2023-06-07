using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Sample.Holion.D.SupportTicket;
using Timeout = Cleipnir.ResilientFunctions.Domain.Events.Timeout;

namespace Sample.Holion.Solutions.D;

public class SupportTicketFlow : Flow<SupportTicketRequest, SupportTicketFlow.SupportTicketScrapbook>
{
    public override async Task Run(SupportTicketRequest request)
    {
        var agents = request.CustomerSupportAgents.Length;
        while (true)
        {
            if (!TimeoutOrResponseForTryReceived(EventSource, Scrapbook.Try)) //then send email requesting support ticket to be taken
            {
                var customerSupportAgentEmail = request.CustomerSupportAgents[Scrapbook.Try % agents];
                await MessageBroker.Send(new TakeSupportTicket(request.SupportTicketId, customerSupportAgentEmail, RequestId: Scrapbook.Try.ToString()));
                await EventSource.TimeoutProvider.RegisterTimeout(timeoutId: Scrapbook.Try.ToString(), expiresIn: TimeSpan.FromSeconds(5));                
            }
            
            var either = await EventSource
                .OfTypes<SupportTicketTaken, Timeout>()
                .Where(e => e.Match(stt => int.Parse(stt.RequestId), t => int.Parse(t.TimeoutId)) == Scrapbook.Try)
                .SuspendUntilNext();

            if (either.HasFirst)
                return;

            Scrapbook.Try++;
            await Scrapbook.Save();
        }
    }

    public class SupportTicketScrapbook : RScrapbook
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