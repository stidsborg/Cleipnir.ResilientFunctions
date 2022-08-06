using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class PostponedTests : TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionIsCompletedAfterInMemoryTimeout()
        => PostponedActionIsCompletedAfterInMemoryTimeout(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionIsNotInvokedOnHigherVersion()
        => PostponedActionIsNotInvokedOnHigherVersion(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedAction()
        => ThrownPostponeExceptionResultsInPostponedAction(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedActionWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedActionWithScrapbook(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFunc()
        => ThrownPostponeExceptionResultsInPostponedFunc(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}