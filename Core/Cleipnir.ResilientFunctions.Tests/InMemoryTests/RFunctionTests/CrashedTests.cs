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
        => NonCompletedFuncIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog()
        => NonCompletedFuncWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task NonCompletedActionIsCompletedByWatchDog()
        => NonCompletedActionIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task NonCompletedActionWithScrapbookIsCompletedByWatchDog()
        => NonCompletedActionWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task CrashedActionReInvocationModeShouldBeRetry()
        => CrashedActionReInvocationModeShouldBeRetry(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}