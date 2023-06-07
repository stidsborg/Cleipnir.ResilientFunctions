using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.D.SupportTicket;

public class SupportTicketFlows
{
    private readonly RAction<SupportTicketRequest, SupportTicketFlow.SupportTicketScrapbook> _registration;
    public SupportTicketFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<SupportTicketFlow>()
            .RegisterAction<SupportTicketRequest, SupportTicketFlow.SupportTicketScrapbook>(
                functionTypeId: nameof(SupportTicketFlow),
                flow => (param, scrapbook, context) => PrepareAndExecute(flow, param, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<SupportTicketRequest, SupportTicketFlow.SupportTicketScrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, SupportTicketRequest supportTicketRequest, SupportTicketFlow.SupportTicketScrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, supportTicketRequest, scrapbook);

    public Task Schedule(string instanceId, SupportTicketRequest supportTicketRequest, SupportTicketFlow.SupportTicketScrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, supportTicketRequest, scrapbook);

    private async Task PrepareAndExecute(SupportTicketFlow flow, SupportTicketRequest supportTicketRequest, SupportTicketFlow.SupportTicketScrapbook scrapbook, Context context)
    {
        typeof(SupportTicketFlow).GetProperty(nameof(Flow<Unit>.Context))!.SetValue(flow, context);
        typeof(SupportTicketFlow).GetProperty(nameof(Flow<Unit>.Scrapbook))!.SetValue(flow, scrapbook);
        var eventSource = await context.EventSource;
        typeof(SupportTicketFlow).GetProperty(nameof(Flow<Unit>.EventSource))!.SetValue(flow, eventSource);
        typeof(SupportTicketFlow).GetProperty(nameof(Flow<Unit>.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Run(supportTicketRequest);
    }
}