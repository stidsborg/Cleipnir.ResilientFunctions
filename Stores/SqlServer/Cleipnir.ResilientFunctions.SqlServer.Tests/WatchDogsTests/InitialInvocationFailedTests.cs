using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests;

[TestClass]
public class InitialInvocationFailedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.InitialInvocationFailedTests
{
    [TestMethod]
    public override Task CreatedActionIsCompletedByWatchdog()
        => CreatedActionIsCompletedByWatchdog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CreatedActionWithScrapbookIsCompletedByWatchdog()
        => CreatedActionWithScrapbookIsCompletedByWatchdog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CreatedFuncIsCompletedByWatchdog()
        => CreatedFuncIsCompletedByWatchdog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CreatedFuncWithScrapbookIsCompletedByWatchdog()
        => CreatedFuncWithScrapbookIsCompletedByWatchdog(FunctionStoreFactory.Create());
}