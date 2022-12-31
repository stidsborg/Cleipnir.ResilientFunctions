using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Sample.WebApi.OrderProcessing.Communication.Messaging;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.MessageBased;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    private const int MaxWaitMs = 5_000;
    
    public RAction.Invoke<Order, Scrapbook> ProcessOrder { get; }
    public ControlPanels<Order, Scrapbook> ControlPanels { get; }
    private readonly EventSourceWriters _eventSourceWriters;

    public OrderProcessor(RFunctions rFunctions, MessageBroker messageBroker)
    {
        var registration = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order, Scrapbook>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        ProcessOrder = registration.Invoke;
        ControlPanels = registration.ControlPanels;
        _eventSourceWriters = registration.EventSourceWriters;

        messageBroker.Subscribe(HandleMessage);
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
        
        await _eventSourceWriters.For(orderId).AppendEvent(@event, idempotencyKey: null);
    }

    public class Inner
    {
        private readonly IOrdersRepository _ordersRepository;
        private readonly MessageBroker _messageBroker;

        public Inner(IOrdersRepository ordersRepository, MessageBroker messageBroker)
        {
            _ordersRepository = ordersRepository;
            _messageBroker = messageBroker;
        }
        
        public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
        {
            var transactionId = scrapbook.TransactionId;
            using var eventSource = await context.EventSource;

            await _messageBroker.Send(new GetProductsTotalPrice(order.OrderId, order.ProductIds));
            var totalPrice = await eventSource
                .OfType<ProductsTotalPrice>()
                .Select(p => p.TotalPrice)
                .Next(MaxWaitMs);

            await _messageBroker.Send(new ReserveFunds(order.OrderId, totalPrice, transactionId, order.CustomerId));
            await eventSource.OfType<FundsReserved>().Next(MaxWaitMs);
        
            await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
            await eventSource.OfType<ProductsShipped>().Next(MaxWaitMs);

            await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
            await eventSource.OfType<OrderConfirmationEmailSent>().Next(MaxWaitMs);
        
            await _messageBroker.Send(new CaptureFunds(order.OrderId, transactionId));
            await eventSource.OfType<FundsCaptured>().Next(MaxWaitMs);
        
            await _ordersRepository.Insert(order);
        }
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; init; } = Guid.NewGuid();
    }
}