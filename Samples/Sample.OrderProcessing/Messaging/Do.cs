using Cleipnir.ResilientFunctions;

namespace Sample.OrderProcessing.Messaging;

public static class Do
{
    public static async Task Execute(RFunctions rFunctions)
    {
        var messageBroker = new MessageBroker();
        var emailService = new EmailServiceStub(messageBroker);
        var logisticsService = new LogisticsServiceStub(messageBroker);
        var paymentProviderService = new PaymentProviderStub(messageBroker);
        
        var orderProcessor = new OrderProcessor(messageBroker);
        var rAction = rFunctions.RegisterAction<Order, OrderProcessor.Scrapbook>(
            "OrderProcessorMessaging",
            orderProcessor.ProcessOrder
        );        
        
        messageBroker.Subscribe(async msg =>
        {
            switch (msg)
            {
                case FundsCaptured e:
                    await rAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsCaptured)}.{e.OrderId}");
                    break;
                case FundsReservationCancelled e:
                    await rAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReservationCancelled)}.{e.OrderId}");
                    break;
                case FundsReserved e:
                    await rAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(FundsReserved)}.{e.OrderId}");
                    break;
                case OrderConfirmationEmailSent e:
                    await rAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(OrderConfirmationEmailSent)}.{e.OrderId}");
                    break;
                case ProductsShipped e:
                    await rAction.EventSourceWriters.For(e.OrderId).AppendEvent(e, idempotencyKey: $"{nameof(ProductsShipped)}.{e.OrderId}");
                    break;

                default:
                    return;
            }
        });

        var order = new Order(
            OrderId: "MK-4321",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid(), Guid.NewGuid() },
            TotalPrice: 123.5M
        );
        await rAction.Invoke(order.OrderId, order);
    }
}