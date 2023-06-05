using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.A.Ordering;

public class OrderFlows
{
    private readonly RAction<Order, OrderFlow.OrderScrapbook> _registration;
    public OrderFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<OrderFlow>()
            .RegisterAction<Order, OrderFlow.OrderScrapbook>(
                functionTypeId: nameof(OrderFlow),
                flow => (order, scrapbook, context) => PrepareAndExecute(flow, order, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<Order, OrderFlow.OrderScrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, Order order, OrderFlow.OrderScrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, order, scrapbook);

    public Task Schedule(string instanceId, Order order, OrderFlow.OrderScrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, order, scrapbook);

    private async Task PrepareAndExecute(OrderFlow flow, Order order, OrderFlow.OrderScrapbook orderScrapbook, Context context)
    {
        typeof(OrderFlow).GetProperty(nameof(Flow<Unit>.Context))!.SetValue(flow, context);
        typeof(OrderFlow).GetProperty(nameof(Flow<Unit>.Scrapbook))!.SetValue(flow, orderScrapbook);
        var eventSource = await context.EventSource;
        typeof(OrderFlow).GetProperty(nameof(Flow<Unit>.EventSource))!.SetValue(flow, eventSource);
        typeof(OrderFlow).GetProperty(nameof(Flow<Unit>.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Run(order);
    }
}