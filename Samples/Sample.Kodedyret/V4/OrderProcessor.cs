using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Serilog;

namespace Sample.Kodedyret.V4;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    public RAction.Invoke<Order, Scrapbook> ProcessOrder { get; }

    public OrderProcessor(RFunctions rFunctions, MessageBroker messageBroker)
    {
        var rAction = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order, Scrapbook>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        ProcessOrder = rAction.Invoke;

        messageBroker.Subscribe(async msg =>
        {
            switch (msg)
            {
                case FundsCaptured e:
                    await rAction.EventSourceWriters.For(e.OrderId).Append(e.OrderId, e.OrderId);
                    break;
                case FundsReservationCancelled e:
                    await rAction.EventSourceWriters.For(e.OrderId).Append(e.OrderId, e.OrderId);
                    break;
                case FundsReserved e:
                    await rAction.EventSourceWriters.For(e.OrderId).Append(e.OrderId, e.OrderId);
                    break;
                case OrderConfirmationEmailSent e:
                    await rAction.EventSourceWriters.For(e.OrderId).Append(e.OrderId, e.OrderId);
                    break;
                case ProductsShipped e:
                    await rAction.EventSourceWriters.For(e.OrderId).Append(e.OrderId, e.OrderId);
                    break;

                default:
                    return;
            }
        });
    }

    public class Inner
    {
        private readonly MessageBroker _messageBroker;

        public Inner(MessageBroker messageBroker) => _messageBroker = messageBroker;

        public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
        {
            Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            using var eventSource = await context.EventSource;

            await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
            await eventSource.OfType<FundsReserved>().Next(maxWaitMs: 5_000);
            
            await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
            await eventSource.OfType<ProductsShipped>().Next(maxWaitMs: 5_000);
            
            await _messageBroker.Send(new CaptureFunds(order.OrderId, order.CustomerId, scrapbook.TransactionId));
            await eventSource.OfType<FundsCaptured>().Next(maxWaitMs: 5_000);

            await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
            await eventSource.OfType<OrderConfirmationEmailSent>().Next(5_000);

            Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
        }        
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; } = Guid.NewGuid();
    }
}