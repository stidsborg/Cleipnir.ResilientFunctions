using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class FailedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.FailedTests
{
    [TestMethod]
    public override Task ExceptionThrowingFuncIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    public override Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExceptionThrowingActionIsNotCompletedByWatchDog()
        => ExceptionThrowingActionIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());
}