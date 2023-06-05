﻿using Cleipnir.ResilientFunctions.Domain;
using Sample.Holion.A.Ordering.Clients;
using Serilog;

namespace Sample.Holion.A.Ordering;

public class OrderFlow : Flow<Order, OrderFlow.OrderScrapbook>
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
        
        var transactionId = Guid.Empty;
        await _paymentProviderClient.Reserve(order.CustomerId, transactionId, order.TotalPrice);

        await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);

        await _paymentProviderClient.Capture(transactionId);

        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

        Log.Logger.ForContext<OrderFlow>().Information($"Processing of order '{order.OrderId}' completed");
    }
    public class OrderScrapbook : RScrapbook {}
}