using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsoleApp.Presentation;

internal class BrokenOrder
{
    private readonly IPaymentProviderClient _paymentProviderClient = null!;
    private readonly ILogisticsClient _logisticsClient = null!;
    private readonly IEmailClient _emailClient = null!;
    
    public async Task ProcessOrder(Order order)
    {
        var transactionId = Guid.NewGuid();
        await _paymentProviderClient.Reserve(transactionId, order.CustomerId, order.TotalPrice);
        var trackAndTrace = await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
        await _paymentProviderClient.Capture(transactionId);
        await _emailClient.SendOrderConfirmation(order.CustomerId, trackAndTrace, order.OrderNumber);
    }

    private void Do()
    {
        var _ = ProcessOrder(null!);
    }
    
    public record Order(Guid OrderNumber, Guid CustomerId, IEnumerable<Guid> ProductIds, decimal TotalPrice);

    private interface IPaymentProviderClient
    {
        Task Reserve(Guid transactionId, Guid customerId, decimal amount);
        Task Capture(Guid transactionId);
    }
    
    private interface ILogisticsClient
    {
        Task<Guid> ShipProducts(Guid customerId, IEnumerable<Guid> productIds);
    }

    private interface IEmailClient
    {
        Task SendOrderConfirmation(Guid customerId, Guid trackAndTrace, Guid orderNumber);
    }
}