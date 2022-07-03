using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Clients;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Domain;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Saga.Commands;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Saga.Events;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Saga;

public class OrderProcessingSaga
{
    private const string FunctionTypeId = "OrderProcessing";
    private readonly FunctionTypeEventSources _eventSources;
    private readonly RAction<Order, Scrapbook> _registration;

    private readonly IBankClient _bankClient;
    private readonly IEmailClient _emailClient;
    private readonly IMessageQueueClient _messageQueueClient;
    private readonly IProductsClient _productsClient;

    public OrderProcessingSaga(
        RFunctions rFunctions, EventSources eventSources, 
        IBankClient bankClient, IEmailClient emailClient, IMessageQueueClient messageQueueClient, 
        IProductsClient productsClient)
    {
        _eventSources = eventSources.For(FunctionTypeId);
        _bankClient = bankClient;
        _emailClient = emailClient;
        _messageQueueClient = messageQueueClient;
        _productsClient = productsClient;
        
        _registration = rFunctions.RegisterAction<Order, Scrapbook>(
            FunctionTypeId,
            _ProcessOrder
        );
    }

    public async Task DeliverAndProcessEvent(string functionInstanceId, object @event, string? idempotencyKey = null)
    {
        using var eventSource = await _eventSources.Get(functionInstanceId);
        await eventSource.Emit(@event, idempotencyKey);
        await _registration.ScheduleReInvocation(
            functionInstanceId,
            expectedStatuses: new[] {Status.Postponed},
            throwOnUnexpectedFunctionState: false
        );
    }

    public Task ProcessOrder(Order order) => _registration.Invoke(order.OrderId, order);
    private async Task _ProcessOrder(Order order, Scrapbook scrapbook)
    {
        if (scrapbook.BankTransactionId == null)
        {
            scrapbook.BankTransactionId = Guid.NewGuid();
            await scrapbook.Save();
        }
        var bankTransactionId = scrapbook.BankTransactionId.Value;
        using var eventSource = await _eventSources.Get(order.OrderId);
        
        var totalPrice = (await _productsClient.GetProductPrices(order.ProductIds)).Sum(p => p.Price);
        await _bankClient.Reserve(bankTransactionId, totalPrice);

        if (!eventSource.Existing.OfType<ProductsShipped>().Any())
        {
            await _messageQueueClient.Send(new ShipProducts(order.OrderId, order.CustomerEmail, order.ProductIds)); //make this at-most-once
            await eventSource.All.OfType<ProductsShipped>().NextEvent(1_000);
        }

        if (!scrapbook.EmailSent)
        {
            await _emailClient.SendOrderConfirmation(order.CustomerEmail, order.ProductIds);
            scrapbook.EmailSent = true;
            await scrapbook.Save();
        }
    }

    private class Scrapbook : RScrapbook
    {
        public Guid? BankTransactionId { get; set; }
        public bool EmailSent { get; set; }
    }
}
