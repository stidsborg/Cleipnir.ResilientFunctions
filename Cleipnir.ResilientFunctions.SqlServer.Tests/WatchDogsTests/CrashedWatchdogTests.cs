using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests;

[TestClass]
public class CrashedWatchdogTests : ResilientFunctions.Tests.TestTemplates.WatchDogsTests.CrashedWatchdogTests
{
    [TestMethod]
    public override Task CrashedFunctionInvocationIsCompletedByWatchDog()
        => CrashedFunctionInvocationIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog()
        => CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CrashedActionInvocationIsCompletedByWatchDog()
        => CrashedFunctionInvocationIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CrashedActionWithScrapbookInvocationIsCompletedByWatchDog()
        => CrashedActionWithScrapbookInvocationIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());
}