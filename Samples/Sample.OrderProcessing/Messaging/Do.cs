using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;

namespace Sample.OrderProcessing.Messaging;

public static class Do
{
    public static async Task Execute(FunctionsRegistry functionsRegistry)
    {
        var messageBroker = new Bus();
        var emailService = new EmailServiceStub(messageBroker);
        var logisticsService = new LogisticsServiceStub(messageBroker);
        var paymentProviderService = new PaymentProviderStub(messageBroker);
        
        var orderProcessor = new OrderProcessor(messageBroker);
        var rAction = functionsRegistry.RegisterAction<Order>(
            "OrderProcessorMessaging",
            orderProcessor.Execute,
            new LocalSettings(messagesDefaultMaxWaitForCompletion: TimeSpan.FromMinutes(1))
        );        
        
        messageBroker.Subscribe(async msg =>
        {
            switch (msg)
            {
                case FundsCaptured e:
                    await rAction.MessageWriters.For(e.OrderId).AppendMessage(e, idempotencyKey: $"{nameof(FundsCaptured)}.{e.OrderId}");
                    break;
                case FundsReservationCancelled e:
                    await rAction.MessageWriters.For(e.OrderId).AppendMessage(e, idempotencyKey: $"{nameof(FundsReservationCancelled)}.{e.OrderId}");
                    break;
                case FundsReserved e:
                    await rAction.MessageWriters.For(e.OrderId).AppendMessage(e, idempotencyKey: $"{nameof(FundsReserved)}.{e.OrderId}");
                    break;
                case OrderConfirmationEmailSent e:
                    await rAction.MessageWriters.For(e.OrderId).AppendMessage(e, idempotencyKey: $"{nameof(OrderConfirmationEmailSent)}.{e.OrderId}");
                    break;
                case ProductsShipped e:
                    await rAction.MessageWriters.For(e.OrderId).AppendMessage(e, idempotencyKey: $"{nameof(ProductsShipped)}.{e.OrderId}");
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
        await rAction.Run(order.OrderId, order);
    }
}