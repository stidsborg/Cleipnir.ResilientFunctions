using System.Reactive.Linq;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.Communication.Messaging;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.MessageBased;

public class OrderProcessor
{
    private readonly IOrdersRepository _ordersRepository;
    
    private readonly FunctionTypeEventSources _eventSources;
    private readonly EventSourceWriter _eventSourceWriter;
    private readonly MessageBroker _messageBroker;
    
    private const int MAX_WAIT_MS = 5_000;
    
    public OrderProcessor(IOrdersRepository ordersRepository, RFunctions rFunctions, EventSources eventSources, MessageBroker messageBroker)
    {
        _ordersRepository = ordersRepository;
        _messageBroker = messageBroker;

        var registration = rFunctions.RegisterAction<OrderAndPaymentProviderTransactionId>(
            nameof(OrderProcessor),
            _ProcessOrder
        );
        ProcessOrder = registration.Invoke;
        
        _eventSources = eventSources.For(nameof(OrderProcessor));
        _eventSourceWriter = _eventSources.CreateWriter(registration.ScheduleReInvocation);
        
        _messageBroker.Subscribe(HandleMessage);
    }

    public RAction.Invoke<OrderAndPaymentProviderTransactionId> ProcessOrder { get; }

    private async Task _ProcessOrder(OrderAndPaymentProviderTransactionId orderAndTransactionId)
    {
        var (order, transactionId) = orderAndTransactionId;
        using var eventSource = await _eventSources.Get(order.OrderId);
        var events = eventSource.All;
        
        await _messageBroker.Send(new GetProductsTotalPrice(order.OrderId, order.ProductIds));
        var totalPrice = (await eventSource.All.OfType<ProductsTotalPrice>().NextEvent(MAX_WAIT_MS)).TotalPrice;
        
        await _messageBroker.Send(new ReserveFunds(order.OrderId, totalPrice, transactionId, order.CustomerId));
        await events.OfType<FundsReserved>().NextEvent(MAX_WAIT_MS);
        
        await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
        await events.OfType<ProductsShipped>().NextEvent(MAX_WAIT_MS);

        await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
        await events.OfType<OrderConfirmationEmailSent>().NextEvent(MAX_WAIT_MS);
        
        await _messageBroker.Send(new CaptureFunds(order.OrderId, transactionId));
        await events.OfType<FundsCaptured>().NextEvent(MAX_WAIT_MS);
        
        await _ordersRepository.Insert(order);
    }
    
    private async Task HandleMessage(EventsAndCommands message)
    {
        var (@event, orderId) = message switch
        {
            FundsCaptured fundsCaptured => (fundsCaptured, fundsCaptured.OrderId),
            FundsReserved fundsReserved => (fundsReserved, fundsReserved.OrderId),
            OrderConfirmationEmailSent orderConfirmationEmailSent => (orderConfirmationEmailSent, orderConfirmationEmailSent.OrderId),
            ProductsShipped productsShipped => (productsShipped, productsShipped.OrderId),
            ProductsTotalPrice productsTotalPrice => (productsTotalPrice, productsTotalPrice.OrderId),
            _ => (default(EventsAndCommands), "")
        };
        if (@event == null) return;

        await _eventSourceWriter.Append(orderId, @event, idempotencyKey: null, awakeIfSuspended: false);
    }
}