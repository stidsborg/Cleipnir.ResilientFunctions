using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.Domain;
using Serilog;

namespace Sample.Kodedyret.V1;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    public RAction.Invoke<Order, RScrapbook> ProcessOrder { get; }

    public OrderProcessor(RFunctions rFunctions)
    {
        var registration = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Order>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        ProcessOrder = registration.Invoke;
    }

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

        public async Task ProcessOrder(Order order)
        {
            Log.Logger.ForContext<OrderProcessor>().Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' started");
            
            var transactionId = await _paymentProviderClient.Reserve(order.CustomerId, order.TotalPrice);
            await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
            await _paymentProviderClient.Capture(transactionId);
            await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);

            Log.Logger.ForContext<OrderProcessor>().Information($"ORDER_PROCESSOR: Processing of order '{order.OrderId}' completed");
        }        
    }
}