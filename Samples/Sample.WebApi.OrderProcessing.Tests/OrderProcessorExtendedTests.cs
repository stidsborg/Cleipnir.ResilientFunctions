using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Sample.WebApi.OrderProcessing.BusinessLogic.RpcBased;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.Domain;
using Shouldly;

namespace Sample.WebApi.OrderProcessing.Tests;

[TestClass]
public class OrderProcessorExtendedTests
{
    [TestMethod]
    public async Task SunshineScenario()
    {
        var paymentProviderClientMock = new Mock<IPaymentProviderClient>();
        var productsClientMock = new Mock<IProductsClient>();
        var emailClientMock = new Mock<IEmailClient>();
        var logisticsClientMock = new Mock<ILogisticsClient>();
        var ordersRepositoryMock = new Mock<IOrdersRepository>();
        var scrapbookMock = new Mock<OrderProcessorExtended.Scrapbook>();
        
        var order = new Order(
            "SomeOrderId",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid() }
        );

        var scrapbook = scrapbookMock.Object;
        var sut = new OrderProcessorExtended(
            paymentProviderClientMock.Object,
            productsClientMock.Object,
            emailClientMock.Object,
            logisticsClientMock.Object,
            ordersRepositoryMock.Object,
            new RFunctions(new InMemoryFunctionStore())
        );
        await sut._ProcessOrder(order, scrapbook);
        
        scrapbookMock.Verify(s => s.Save(), Times.Exactly(4));
    }
    
    [TestMethod]
    public async Task ExceptionIsThrownWhenLogisticsServiceCallHasBeenStartedButNotCompleted()
    {
        var paymentProviderClientMock = new Mock<IPaymentProviderClient>();
        var productsClientMock = new Mock<IProductsClient>();
        var emailClientMock = new Mock<IEmailClient>();
        var logisticsClientMock = new Mock<ILogisticsClient>();
        var ordersRepositoryMock = new Mock<IOrdersRepository>();
        var scrapbookMock = new Mock<OrderProcessorExtended.Scrapbook>();
        
        var order = new Order(
            "SomeOrderId",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid() }
        );

        var scrapbook = scrapbookMock.Object;
        scrapbook.TotalPrice = 100M;
        scrapbook.TransactionId = Guid.NewGuid();
        scrapbook.ShipProductsStatus = OrderProcessorExtended.Status.Started;
        
        var sut = new OrderProcessorExtended(
            paymentProviderClientMock.Object,
            productsClientMock.Object,
            emailClientMock.Object,
            logisticsClientMock.Object,
            ordersRepositoryMock.Object,
            new RFunctions(new InMemoryFunctionStore())
        );
        await Should.ThrowAsync<OrderProcessorExtended.ProductShipmentStartedButNotCompleted>(sut._ProcessOrder(order, scrapbook));
    }
}