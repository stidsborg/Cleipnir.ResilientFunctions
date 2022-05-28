using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.WatchDogsTests;

[TestClass]
public class InitialInvocationFailedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.InitialInvocationFailedTests
{
    [TestMethod]
    public override Task CreatedActionIsCompletedByWatchdog()
        => CreatedActionIsCompletedByWatchdog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CreatedActionWithScrapbookIsCompletedByWatchdog()
        => CreatedActionWithScrapbookIsCompletedByWatchdog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CreatedFuncIsCompletedByWatchdog()
        => CreatedFuncIsCompletedByWatchdog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CreatedFuncWithScrapbookIsCompletedByWatchdog()
        => CreatedFuncWithScrapbookIsCompletedByWatchdog(NoSql.AutoCreateAndInitializeStore());
}