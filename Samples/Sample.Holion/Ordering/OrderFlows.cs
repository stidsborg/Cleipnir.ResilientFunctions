using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.Ordering;

public class OrderFlows
{
    private readonly RAction<Order, Scrapbook> _registration;
    public OrderFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<OrderFlow>()
            .RegisterAction<Order, Scrapbook>(
                functionTypeId: nameof(OrderFlow),
                flow => (order, scrapbook, context) => PrepareAndExecute(flow, order, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<Order, Scrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, Order order, Scrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, order, scrapbook);

    public Task Schedule(string instanceId, Order order, Scrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, order, scrapbook);

    private async Task PrepareAndExecute(OrderFlow flow, Order order, Scrapbook scrapbook, Context context)
    {
        typeof(OrderFlow).GetProperty(nameof(OrderFlow.Context))!.SetValue(flow, context);
        typeof(OrderFlow).GetProperty(nameof(OrderFlow.Scrapbook))!.SetValue(flow, scrapbook);
        var eventSource = await context.EventSource;
        typeof(OrderFlow).GetProperty(nameof(OrderFlow.EventSource))!.SetValue(flow, eventSource);
        typeof(OrderFlow).GetProperty(nameof(OrderFlow.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Invoke(order);
    }
}