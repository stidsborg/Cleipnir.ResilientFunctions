using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ExceptionHandlingTests
{
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
}