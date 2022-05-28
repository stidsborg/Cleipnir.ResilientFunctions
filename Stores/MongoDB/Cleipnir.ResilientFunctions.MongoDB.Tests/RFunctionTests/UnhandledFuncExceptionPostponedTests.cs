using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class UnhandledFuncExceptionPostponedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.UnhandledFuncExceptionPostponedTests
{
    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFunc()
        => UnhandledExceptionResultsInPostponedFunc(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFuncWithScrapbook()
        => UnhandledExceptionResultsInPostponedFuncWithScrapbook(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedAction()
        => UnhandledExceptionResultsInPostponedAction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedActionWithScrapbook()
        => UnhandledExceptionResultsInPostponedActionWithScrapbook(NoSql.AutoCreateAndInitializeStore());
}