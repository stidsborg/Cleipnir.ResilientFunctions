using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Serilog;

namespace Sample.Kodedyret.V4;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    private RAction<Order, Scrapbook> RAction { get; }

    public OrderProcessor(RFunctions rFunctions, MessageBroker messageBroker)
    {
        RAction = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order, Scrapbook>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        messageBroker.Subscribe(async msg =>
        {
            switch (msg)
            {
                case FundsCaptured e:
                    await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsCaptured)}.{e.OrderId}");
                    break;
                case FundsReservationCancelled e:
                    await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReservationCancelled)}.{e.OrderId}");
                    break;
                case FundsReserved e:
                    await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReserved)}.{e.OrderId}");
                    break;
                case OrderConfirmationEmailSent e:
                    await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(OrderConfirmationEmailSent)}.{e.OrderId}");
                    break;
                case ProductsShipped e:
                    await RAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(ProductsShipped)}.{e.OrderId}");
                    break;

                default:
                    return;
            }
        });
    }

    public async Task X(RAction<string> registration)
    {
        var email = "test";
        var welcomeEmailSent = new WelcomeEmailSent();

        var eventSourceWriter = registration.EventSourceWriters.For(email);
        await eventSourceWriter.AppendEvent(welcomeEmailSent);
    }
    
    public Task ProcessOrder(Order order) => RAction.Invoke(order.OrderId, order);

    public class Inner
    {
        private readonly MessageBroker _messageBroker;

        public Inner(MessageBroker messageBroker) => _messageBroker = messageBroker;

        public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
        {
            Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            using var eventSource = await context.EventSource;

            await _messageBroker.Send(new ReserveFunds(order.OrderId, order.TotalPrice, scrapbook.TransactionId, order.CustomerId));
            await eventSource.NextOfType<FundsReserved>(maxWait: TimeSpan.FromSeconds(5));
            
            await _messageBroker.Send(new ShipProducts(order.OrderId, order.CustomerId, order.ProductIds));
            await eventSource.OfType<ProductsShipped>().SuspendUntilNext(TimeSpan.FromSeconds(60));
            
            await _messageBroker.Send(new CaptureFunds(order.OrderId, order.CustomerId, scrapbook.TransactionId));
            await eventSource.OfType<FundsCaptured>().SuspendUntilNext(TimeSpan.FromSeconds(60));

            await _messageBroker.Send(new SendOrderConfirmationEmail(order.OrderId, order.CustomerId));
            await eventSource.OfType<OrderConfirmationEmailSent>().SuspendUntilNext(TimeSpan.FromSeconds(60));

            await SignUserUpToNewsLetter("test", context);
            
            Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
        }

        public async Task SignUserUpToNewsLetter(string email, Context context)
        {
            using var eventSource = await context.EventSource;
            await _messageBroker.Send(new SendWelcomeEmail(email));
            await eventSource.NextOfType<WelcomeEmailSent>();
        }
    }

    private record SendWelcomeEmail(string Address) : EventsAndCommands;
    private record WelcomeEmailSent() : EventsAndCommands;

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; } = Guid.NewGuid();
    }
}