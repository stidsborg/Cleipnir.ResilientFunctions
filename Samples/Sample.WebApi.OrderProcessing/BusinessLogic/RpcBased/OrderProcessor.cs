using Cleipnir.ResilientFunctions;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.RpcBased;

public class OrderProcessor
{
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IProductsClient _productsClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;
    private readonly IOrdersRepository _ordersRepository;

    public OrderProcessor(
        IPaymentProviderClient paymentProviderClient, 
        IProductsClient productsClient, 
        IEmailClient emailClient, 
        ILogisticsClient logisticsClient, 
        IOrdersRepository ordersRepository,
        RFunctions rFunctions)
    {
        _paymentProviderClient = paymentProviderClient;
        _productsClient = productsClient;
        _emailClient = emailClient;
        _logisticsClient = logisticsClient;
        _ordersRepository = ordersRepository;

        var registration = rFunctions.RegisterAction<OrderAndPaymentProviderTransactionId>(
            nameof(OrderProcessor),
            _ProcessOrder
        );
        ProcessOrder = registration.Invoke;
    }

    public RAction.Invoke<OrderAndPaymentProviderTransactionId> ProcessOrder { get; }

    private async Task _ProcessOrder(OrderAndPaymentProviderTransactionId orderAndTransactionId)
    {
        var (order, transactionId) = orderAndTransactionId;

        var prices = await _productsClient.GetProductPrices(order.ProductIds);
        var totalPrice = prices.Sum(p => p.Price);

        await _paymentProviderClient.Reserve(transactionId, totalPrice);
        await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
        await _paymentProviderClient.Capture(transactionId);
        await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);
        await _ordersRepository.Insert(order);
    }
}