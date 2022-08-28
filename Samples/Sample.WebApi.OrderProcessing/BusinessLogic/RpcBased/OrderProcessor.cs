﻿using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.BusinessLogic.RpcBased;

public class OrderProcessor : IRegisterRFuncOnInstantiation
{
    public RAction.Invoke<OrderAndPaymentProviderTransactionId> ProcessOrder { get; }

    public OrderProcessor(RFunctions rFunctions)
    {
        var registration = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<OrderAndPaymentProviderTransactionId>(
                nameof(OrderProcessor),
                inner => inner.ProcessOrder
            );

        ProcessOrder = registration.Invoke;
    }

    public class Inner
    {
        private readonly IPaymentProviderClient _paymentProviderClient;
        private readonly IProductsClient _productsClient;
        private readonly IEmailClient _emailClient;
        private readonly ILogisticsClient _logisticsClient;
        private readonly IOrdersRepository _ordersRepository;

        public Inner(
            IPaymentProviderClient paymentProviderClient, 
            IProductsClient productsClient, 
            IEmailClient emailClient, 
            ILogisticsClient logisticsClient, 
            IOrdersRepository ordersRepository)
        {
            _paymentProviderClient = paymentProviderClient;
            _productsClient = productsClient;
            _emailClient = emailClient;
            _logisticsClient = logisticsClient;
            _ordersRepository = ordersRepository;
        }

        public async Task ProcessOrder(OrderAndPaymentProviderTransactionId orderAndTransactionId)
        {
            Console.WriteLine("HashCode: " + this.GetHashCode());
            var (order, transactionId) = orderAndTransactionId;

            var prices = await _productsClient.GetProductPrices(order.ProductIds);
            var totalPrice = prices.Sum(p => p.Price);

            await _paymentProviderClient.Reserve(transactionId, totalPrice);
            await _logisticsClient.ShipProducts(order.CustomerId, order.ProductIds);
            await _paymentProviderClient.Capture(transactionId);
            await _emailClient.SendOrderConfirmation(order.CustomerId, order.ProductIds);
            await _ordersRepository.Insert(order);

            Console.WriteLine($"Processing of order '{order.OrderId}' completed");
        }        
    }
}