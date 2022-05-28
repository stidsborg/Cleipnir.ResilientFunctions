using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ExceptionHandlingTests
{
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook(NoSql.AutoCreateAndInitializeStore());
}