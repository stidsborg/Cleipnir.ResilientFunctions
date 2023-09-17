using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.WatchDogsTests;

[TestClass]
public class InitialInvocationFailedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.InitialInvocationFailedTests
{
    [TestMethod]
    public override Task CreatedActionIsCompletedByWatchdog()
        => CreatedActionIsCompletedByWatchdog(CreateStore());

    [TestMethod]
    public override Task CreatedActionWithScrapbookIsCompletedByWatchdog()
        => CreatedActionWithScrapbookIsCompletedByWatchdog(CreateStore());

    [TestMethod]
    public override Task CreatedFuncIsCompletedByWatchdog()
        => CreatedFuncIsCompletedByWatchdog(CreateStore());

    [TestMethod]
    public override Task CreatedFuncWithScrapbookIsCompletedByWatchdog()
        => CreatedFuncWithScrapbookIsCompletedByWatchdog(CreateStore());

    private Task<IFunctionStore> CreateStore() => FunctionStoreFactory.Create();
}