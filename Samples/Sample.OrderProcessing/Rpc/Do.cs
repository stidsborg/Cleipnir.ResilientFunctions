using Cleipnir.ResilientFunctions;

namespace Sample.OrderProcessing.Rpc;

public static class Do
{
    public static ActionRegistration<Order> Register(FunctionsRegistry functionsRegistry)
    {
        var orderProcessor = new OrderProcessor(
            new PaymentProviderClientStub(),
            new EmailClientStub(),
            new LogisticsClientStub()
        );

        return functionsRegistry.RegisterAction<Order>(
            "OrderProcessorRpc",
            orderProcessor.Execute
        );
    }

    public static async Task Execute(ActionRegistration<Order> rAction)
    {
        var order = new Order(
            OrderId: "MK-4321",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
            TotalPrice: 123.5M
        );
        await rAction.Run(order.OrderId, order);
    }
}