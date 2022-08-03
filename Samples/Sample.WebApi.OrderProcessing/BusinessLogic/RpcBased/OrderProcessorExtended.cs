using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.RpcBased;

public class OrderProcessorExtended
{
    private readonly IPaymentProviderClient _paymentProviderClient;
    private readonly IProductsClient _productsClient;
    private readonly IEmailClient _emailClient;
    private readonly ILogisticsClient _logisticsClient;
    private readonly IOrdersRepository _ordersRepository;

    public OrderProcessorExtended(
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

        var registration = rFunctions.RegisterAction<Order, Scrapbook>(nameof(OrderProcessor), _ProcessOrder);
        ProcessOrder = registration.Invoke;
    }

    public RAction.Invoke<Order> ProcessOrder { get; }

    internal async Task _ProcessOrder(Order order, Scrapbook scrapbook)
    {
        if (scrapbook.TotalPrice == null)
        {
            var prices = await _productsClient.GetProductPrices(order.ProductIds);
            scrapbook.TotalPrice = prices.Sum(p => p.Price);
            scrapbook.TransactionId = Guid.NewGuid();
            await scrapbook.Save();
        }

        var totalPrice = scrapbook.TotalPrice.Value;
        var transactionId = scrapbook.TransactionId;
        
        await _paymentProviderClient.Reserve(transactionId, totalPrice);

        if (scrapbook.ShipProductsStatus == Status.Started)
            throw new ProductShipmentStartedButNotCompleted();
        
        if (scrapbook.ShipProductsStatus == Status.NotStarted)
        {
            scrapbook.ShipProductsStatus = Status.Started;
            await scrapbook.Save();
            await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
            scrapbook.ShipProductsStatus = Status.Completed;
            await scrapbook.Save();
        }

        await _paymentProviderClient.Capture(transactionId);
        if (!scrapbook.EmailSent)
        {
            await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);
            scrapbook.EmailSent = true;
            await scrapbook.Save();
        }
        
        await _ordersRepository.Insert(order);
    }
    
    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; }
        public decimal? TotalPrice { get; set; }
        public Status ShipProductsStatus { get; set; }
        public bool EmailSent { get; set; }
    }

    public enum Status
    {
        NotStarted = 0,
        Started = 1,
        Completed = 2
    }

    public class ProductShipmentStartedButNotCompleted : Exception
    {
        public ProductShipmentStartedButNotCompleted() 
            : base("ShipProducts started but did not complete before crash") { }
    }
}