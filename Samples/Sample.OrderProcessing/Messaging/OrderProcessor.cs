using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Serilog;

namespace Sample.OrderProcessing.Messaging;

public class OrderProcessor
{
    private ILogger Logger => Log.Logger.ForContext<OrderProcessor>();
    private readonly Bus _bus;

    public OrderProcessor(Bus bus) => _bus = bus;

    public async Task Execute(Order order, Workflow workflow)
    {
        var transactionId = await workflow.Effect.Capture(Guid.NewGuid);
        Logger.Information($"Processing of order '{order.OrderId}' started");
        var messages = workflow.Messages;

        await _bus.Send(new ReserveFunds(order.OrderId, order.TotalPrice, transactionId, order.CustomerId));
        await messages.FirstOfType<FundsReserved>();

        await _bus.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
        var productsShipped = await messages.FirstOfType<ProductsShipped>();

        await _bus.Send(new CaptureFunds(order.OrderId, order.CustomerId, transactionId));
        await messages.FirstOfType<FundsCaptured>();

        await _bus.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId, productsShipped.TrackAndTraceNumber));
        await messages.FirstOfType<OrderConfirmationEmailSent>();

        Logger.Information($"Processing of order '{order.OrderId}' completed");
    }
}
