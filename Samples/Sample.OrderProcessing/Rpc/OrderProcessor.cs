using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Serilog;

namespace Sample.OrderProcessing.Rpc;

public class OrderProcessor
{
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;

    private ILogger Logger => Log.Logger.ForContext<OrderProcessor>();

    public OrderProcessor(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
    {
        _paymentProviderClient = paymentProviderClient;
        _emailClient = emailClient;
        _logisticsClient = logisticsClient;
    }

    public async Task Execute(Order order, Workflow workflow)
    {
        Logger.Information($"Processing of order '{order.OrderId}' started");
        
        var transactionId = await workflow.Activities.Do("TransactionId", Guid.NewGuid);
        await _paymentProviderClient.Reserve(order.CustomerId, transactionId, order.TotalPrice);

        var trackAndTrace = await workflow.Activities.Do(
            "ShipProducts",
            work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds),
            ResiliencyLevel.AtMostOnce
        );

        await _paymentProviderClient.Capture(transactionId);

        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds, trackAndTrace);

        Logger.Information($"Processing of order '{order.OrderId}' completed");
    }
}