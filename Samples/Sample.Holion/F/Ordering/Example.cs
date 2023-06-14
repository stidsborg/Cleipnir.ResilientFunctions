namespace Sample.Holion.F.Ordering;

public static class Example
{
    public static async Task Perform(Flows flows)
    {
        var orderFlows = flows.OrderFlowsWithVersioning;
        var order = new Order("MK-54321", CustomerId: Guid.NewGuid(), ProductIds: new[] { Guid.NewGuid()}, 100M);
        await orderFlows.Run(order.OrderId, order);
    }
}