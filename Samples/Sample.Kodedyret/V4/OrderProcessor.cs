using System.Reactive.Linq;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Serilog;

namespace Sample.Kodedyret.V4;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    public RAction.Invoke<Order, Scrapbook> ProcessOrder { get; }

    public OrderProcessor(RFunctions rFunctions)
    {
        var registration = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order, Scrapbook>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        ProcessOrder = registration.Invoke;
    }

    public class Inner
    {
        private readonly MessageBroker _messageBroker;
        private readonly FunctionTypeEventSources _eventSources;

        public Inner(MessageBroker messageBroker, EventSources eventSources)
        {
            _messageBroker = messageBroker;
            _eventSources = eventSources.For(nameof(OrderProcessor));
        }

        public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
        {
            Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            var eventSource = await _eventSources.Get(order.OrderId);

            await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
            await eventSource.All.OfType<FundsCaptured>().NextEvent(maxWaitMs: 5_000);
            
            await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
            await eventSource.All.OfType<ProductsShipped>().NextEvent(maxWaitMs: 5_000);
            
            await _messageBroker.Send(new CaptureFunds(order.OrderId, scrapbook.TransactionId));
            await eventSource.All.OfType<FundsCaptured>().NextEvent(maxWaitMs: 5_000);

            await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
            await eventSource.All.OfType<OrderConfirmationEmailSent>().NextEvent(5_000);

            Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
        }        
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; }
    }
}