using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.ExceptionHandlingTests
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