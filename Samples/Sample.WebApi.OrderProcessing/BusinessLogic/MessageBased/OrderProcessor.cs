using System.Reactive.Linq;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Sample.WebApi.OrderProcessing.Communication.Messaging;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.MessageBased;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    private const int MaxWaitMs = 5_000;
    
    public RAction.Invoke<Order, Scrapbook> ProcessOrder { get; }
    public ControlPanelFactory<Order, Scrapbook> ControlPanelFactory { get; }
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
        ControlPanelFactory = registration.ControlPanel;
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
        
        await _eventSourceWriters.For(orderId).Append(@event, idempotencyKey: null);
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
            var events = eventSource.All;

            await _messageBroker.Send(new GetProductsTotalPrice(order.OrderId, order.ProductIds));
            var totalPrice = await eventSource
                .All
                .OfType<ProductsTotalPrice>()
                .NextEvent(MaxWaitMs)
                .AfterDo(p => p.TotalPrice);
        
            await _messageBroker.Send(new ReserveFunds(order.OrderId, totalPrice, transactionId, order.CustomerId));
            await events.OfType<FundsReserved>().NextEvent(MaxWaitMs);
        
            await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
            await events.OfType<ProductsShipped>().NextEvent(MaxWaitMs);

            await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
            await events.OfType<OrderConfirmationEmailSent>().NextEvent(MaxWaitMs);
        
            await _messageBroker.Send(new CaptureFunds(order.OrderId, transactionId));
            await events.OfType<FundsCaptured>().NextEvent(MaxWaitMs);
        
            await _ordersRepository.Insert(order);
        }
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; init; } = Guid.NewGuid();
    }
}