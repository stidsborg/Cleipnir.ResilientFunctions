﻿using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Serilog;

namespace Sample.OrderProcessing.Rpc;

public class OrderProcessor
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

    public async Task Execute(Order order, Scrapbook scrapbook, Workflow workflow)
    {
        Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");

        await _paymentProviderClient.Reserve(order.CustomerId, scrapbook.TransactionId, order.TotalPrice);

        var trackAndTrace = await workflow.Activities.Do(
            "ShipProducts",
            work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds)
        );

        await _paymentProviderClient.Capture(scrapbook.TransactionId);

        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds, trackAndTrace);

        Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; } = Guid.NewGuid();
        public Work<TrackAndTrace> ProductsShippedStatus { get; set; }
    }
}