using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class CrashedTests : TestTemplates.RFunctionTests.CrashedTests
{
    [TestMethod]
    public override Task NonCompletedFuncIsCompletedByWatchDog()
        => NonCompletedFuncIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog()
        => NonCompletedFuncWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedActionIsCompletedByWatchDog()
        => NonCompletedActionIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedActionWithScrapbookIsCompletedByWatchDog()
        => NonCompletedActionWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CrashedActionReInvocationModeShouldBeRetry()
        => CrashedActionReInvocationModeShouldBeRetry(FunctionStoreFactory.Create());
}