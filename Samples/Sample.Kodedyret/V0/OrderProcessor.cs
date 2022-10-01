using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Serilog;

namespace Sample.Kodedyret.V0;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;

    public OrderProcessor(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
    {
        _paymentProviderClient = paymentProviderClient;
        _emailClient = emailClient;
        _logisticsClient = logisticsClient;
    }

    public async Task ProcessOrder(Order order)
    {
        Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

        var transactionId = await _paymentProviderClient.Reserve(order.TotalPrice);
        await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
        await _paymentProviderClient.Capture(transactionId);
        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

        Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' completed");
    }
}