using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Cleipnir.ResilientFunctions.Storage;
using Serilog;

namespace Sample.OrderProcessing.Rpc;

public static class Do
{
    public static async Task Execute(FunctionsRegistry functionsRegistry)
    {
        var orderProcessor = new OrderProcessor(
            new PaymentProviderClientStub(),
            new EmailClientStub(),
            new LogisticsClientStub()
        );
        var rAction = functionsRegistry.RegisterAction<Order, OrderProcessor.Scrapbook>(
            "OrderProcessorRpc",
            orderProcessor.ProcessOrder
        );

        var order = new Order(
            OrderId: "MK-4321",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
            TotalPrice: 123.5M
        );
        await rAction.Invoke(order.OrderId, order);
    }
}