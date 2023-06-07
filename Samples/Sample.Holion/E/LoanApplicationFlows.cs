using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.E;

public class LoanApplicationFlows
{
    private readonly RAction<LoanApplication, RScrapbook> _registration;
    public LoanApplicationFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<LoanApplicationFlow>()
            .RegisterAction<LoanApplication, RScrapbook>(
                functionTypeId: nameof(LoanApplicationFlow),
                flow => (param, scrapbook, context) => PrepareAndExecute(flow, param, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<LoanApplication, RScrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, LoanApplication supportTicketRequest, RScrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, supportTicketRequest, scrapbook);

    public Task Schedule(string instanceId, LoanApplication supportTicketRequest, RScrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, supportTicketRequest, scrapbook);

    private async Task PrepareAndExecute(LoanApplicationFlow flow, LoanApplication supportTicketRequest, RScrapbook scrapbook, Context context)
    {
        typeof(LoanApplicationFlow).GetProperty(nameof(Flow<Unit>.Context))!.SetValue(flow, context);
        typeof(LoanApplicationFlow).GetProperty(nameof(Flow<Unit>.Scrapbook))!.SetValue(flow, scrapbook);
        var eventSource = await context.EventSource;
        typeof(LoanApplicationFlow).GetProperty(nameof(Flow<Unit>.EventSource))!.SetValue(flow, eventSource);
        typeof(LoanApplicationFlow).GetProperty(nameof(Flow<Unit>.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Run(supportTicketRequest);
    }
}