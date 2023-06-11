using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Timeout = Cleipnir.ResilientFunctions.Domain.Events.Timeout;

namespace Sample.Holion.D.SupportTicket;

public class SupportTicketFlow : Flow<SupportTicketRequest, SupportTicketFlow.SupportTicketScrapbook>
{
    public override async Task Run(SupportTicketRequest request)
    {
        var agents = request.CustomerSupportAgents.Length;

        await MessageBroker.Send(
            new TakeSupportTicket(request.SupportTicketId, "some@email.com", RequestId: Scrapbook.Try.ToString())
        );
        await Task.CompletedTask;
        
        throw new NotImplementedException();
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