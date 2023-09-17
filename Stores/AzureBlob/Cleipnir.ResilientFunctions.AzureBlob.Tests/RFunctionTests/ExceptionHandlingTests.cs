using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ExceptionHandlingTests
{
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook(FunctionStoreFactory.Create());
}