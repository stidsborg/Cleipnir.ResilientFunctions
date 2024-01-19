﻿using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Serilog;

namespace Sample.OrderProcessing.Messaging;

public class OrderProcessor
{
    private readonly MessageBroker _messageBroker;

    public OrderProcessor(MessageBroker messageBroker) => _messageBroker = messageBroker;

    public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
    {
        Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
        using var messages = context.Messages;

        await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
        await messages.FirstOfType<FundsReserved>();

        await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
        var productsShipped = await messages.FirstOfType<ProductsShipped>();

        await _messageBroker.Send(new CaptureFunds(order.OrderId, order.CustomerId, scrapbook.TransactionId));
        await messages.FirstOfType<FundsCaptured>();

        await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId, productsShipped.TrackAndTraceNumber));
        await messages.FirstOfType<OrderConfirmationEmailSent>();

        Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
    }


    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; } = Guid.NewGuid();
    }
}
