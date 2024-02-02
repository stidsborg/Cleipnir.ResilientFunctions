using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class PostponedTests : TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedAction()
        => ThrownPostponeExceptionResultsInPostponedAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedActionWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedActionWithScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFunc()
        => ThrownPostponeExceptionResultsInPostponedFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp()
        => ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleAtActionIsCompletedAfterDelay()
        => ScheduleAtActionIsCompletedAfterDelay(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleAtFuncIsCompletedAfterDelay()
        => ScheduleAtActionIsCompletedAfterDelay(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ContextDelayInvocationDelaysFunction()
        => ContextDelayInvocationDelaysFunction(FunctionStoreFactory.Create());
}