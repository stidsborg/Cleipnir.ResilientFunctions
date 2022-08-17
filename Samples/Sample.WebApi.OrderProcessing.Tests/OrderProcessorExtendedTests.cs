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
        var scrapbookMock = new Mock<OrderProcessorExtended.Inner.Scrapbook>();
        
        var order = new Order(
            "SomeOrderId",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid() }
        );

        var scrapbook = scrapbookMock.Object;
        var sut = new OrderProcessorExtended.Inner(
            paymentProviderClientMock.Object,
            productsClientMock.Object,
            emailClientMock.Object,
            logisticsClientMock.Object,
            ordersRepositoryMock.Object
        );
        await sut.ProcessOrder(order, scrapbook);
        
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
        var scrapbookMock = new Mock<OrderProcessorExtended.Inner.Scrapbook>();
        
        var order = new Order(
            "SomeOrderId",
            CustomerId: Guid.NewGuid(),
            ProductIds: new[] { Guid.NewGuid() }
        );

        var scrapbook = scrapbookMock.Object;
        scrapbook.TotalPrice = 100M;
        scrapbook.TransactionId = Guid.NewGuid();
        scrapbook.ShipProductsStatus = OrderProcessorExtended.Inner.Status.Started;
        
        var sut = new OrderProcessorExtended.Inner(
            paymentProviderClientMock.Object,
            productsClientMock.Object,
            emailClientMock.Object,
            logisticsClientMock.Object,
            ordersRepositoryMock.Object
        );
        await Should.ThrowAsync<OrderProcessorExtended.Inner.ProductShipmentStartedButNotCompleted>(sut.ProcessOrder(order, scrapbook));
    }
}