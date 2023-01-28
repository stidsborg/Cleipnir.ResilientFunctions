using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Serilog;

namespace Sample.Kodedyret.V3;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    private readonly RAction<Order, Scrapbook> _rAction;

    public OrderProcessor(RFunctions rFunctions)
    {
        _rAction = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order, Scrapbook>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );
    }

    public Task ProcessOrder(Order order) => _rAction.Invoke(order.OrderId, order);
    
    public class Inner
    {
        private readonly IPaymentProviderClient _paymentProviderClient;
        private readonly IEmailClient _emailClient;
        private readonly ILogisticsClient _logisticsClient;

        public Inner(IPaymentProviderClient paymentProviderClient, IEmailClient emailClient, ILogisticsClient logisticsClient)
        {
            _paymentProviderClient = paymentProviderClient;
            _emailClient = emailClient;
            _logisticsClient = logisticsClient;
        }

        public async Task ProcessOrder(Order order, Scrapbook scrapbook, Context context)
        {
            Log.Logger.Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            
            await _paymentProviderClient.Reserve(order.CustomerId, scrapbook.TransactionId, order.TotalPrice);
            
            await scrapbook.DoAtMostOnce(
                workStatus: s => s.ProductsShippedStatus,
                work: () => _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds)
            );

            await _paymentProviderClient.Capture(scrapbook.TransactionId);            

            await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

            Log.Logger.ForContext<OrderProcessor>().Information($"Processing of order '{order.OrderId}' completed");
        }        
    }

    public class Scrapbook : RScrapbook
    {
        public Guid TransactionId { get; set; } = Guid.NewGuid();
        public WorkStatus ProductsShippedStatus { get; set; }
    }
}