using Cleipnir.ResilientFunctions.Domain;
using Sample.Holion.A.Ordering.Clients;
using Serilog;

namespace Sample.Holion.A.Ordering;

public class OrderFlow : Flow<Order, Scrapbook>
{
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;

    public OrderFlow(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
    {
        _paymentProviderClient = paymentProviderClient;
        _emailClient = emailClient;
        _logisticsClient = logisticsClient;
    }

    public override async Task Run(Order order)
    {
        Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
        
        await _paymentProviderClient.Reserve(order.CustomerId, Scrapbook.TransactionId, order.TotalPrice);

        await DoAtMostOnce(
            workStatus: s => s.ProductsShippedStatus,
            work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds)
        );

        await _paymentProviderClient.Capture(Scrapbook.TransactionId);

        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

        Log.Logger.ForContext<OrderFlow>().Information($"Processing of order '{order.OrderId}' completed");
    }
}

public class Scrapbook : RScrapbook
{
    public Guid TransactionId { get; set; } 
    public WorkStatus ProductsShippedStatus { get; set; }
}