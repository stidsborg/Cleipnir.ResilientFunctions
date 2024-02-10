using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ExceptionHandlingTests
{
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithState()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithState(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithState()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithState(FunctionStoreFactory.Create());
}