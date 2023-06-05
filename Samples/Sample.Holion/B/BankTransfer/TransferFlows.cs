using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.B.BankTransfer;

public class TransferFlows
{
    private readonly RAction<Transfer, RScrapbook> _registration;
    public TransferFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<TransferFlow>()
            .RegisterAction<Transfer, RScrapbook>(
                functionTypeId: nameof(TransferFlow),
                flow => (order, scrapbook, context) => PrepareAndExecute(flow, order, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<Transfer, RScrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, Transfer transfer, RScrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, transfer, scrapbook);

    public Task Schedule(string instanceId, Transfer transfer, RScrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, transfer, scrapbook);

    private async Task PrepareAndExecute(TransferFlow flow, Transfer order, RScrapbook scrapbook, Context context)
    {
        typeof(TransferFlow).GetProperty(nameof(Flow<Unit>.Context))!.SetValue(flow, context);
        typeof(TransferFlow).GetProperty(nameof(Flow<Unit>.Scrapbook))!.SetValue(flow, scrapbook);
        var eventSource = await context.EventSource;
        typeof(TransferFlow).GetProperty(nameof(Flow<Unit>.EventSource))!.SetValue(flow, eventSource);
        typeof(TransferFlow).GetProperty(nameof(Flow<Unit>.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Run(order);
    }
}