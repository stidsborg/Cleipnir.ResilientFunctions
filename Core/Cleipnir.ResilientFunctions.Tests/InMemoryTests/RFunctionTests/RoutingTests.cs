using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class RoutingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.RoutingTests
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
}