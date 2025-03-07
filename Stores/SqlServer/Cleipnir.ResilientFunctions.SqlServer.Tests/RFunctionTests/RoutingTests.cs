using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class RoutingTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.RoutingTests
{
    [TestMethod]
    public override Task MessageIsRoutedToParamlessInstance()
        => MessageIsRoutedToParamlessInstance(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task MessageIsRoutedToActionInstance()
        => MessageIsRoutedToActionInstance(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessageIsRoutedToFuncInstance()
        => MessageIsRoutedToFuncInstance(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessageIsRoutedUsingRoutingInfo()
        => MessageIsRoutedUsingRoutingInfo(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessageIsRoutedToParamlessInstanceUsingCorrelationId()
        => MessageIsRoutedToParamlessInstanceUsingCorrelationId(FunctionStoreFactory.Create());

    
    [TestMethod]
    public override Task MessageIsRoutedToMultipleInstancesUsingCorrelationId()
        => MessageIsRoutedToMultipleInstancesUsingCorrelationId(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParamlessInstanceIsStartedByMessage()
        => ParamlessInstanceIsStartedByMessage(FunctionStoreFactory.Create());
}