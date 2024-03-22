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
    public override Task PostponedFuncWithStateIsCompletedByWatchDog()
        => PostponedFuncWithStateIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedActionWithStateIsCompletedByWatchDog()
        => PostponedActionWithStateIsCompletedByWatchDog(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedAction()
        => ThrownPostponeExceptionResultsInPostponedAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedActionWithState()
        => ThrownPostponeExceptionResultsInPostponedActionWithState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFunc()
        => ThrownPostponeExceptionResultsInPostponedFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFuncWithState()
        => ThrownPostponeExceptionResultsInPostponedFuncWithState(FunctionStoreFactory.Create());

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
    public override Task WorkflowDelayInvocationDelaysFunction()
        => WorkflowDelayInvocationDelaysFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowDelayWithDateTimeInvocationDelaysFunction()
        => WorkflowDelayWithDateTimeInvocationDelaysFunction(FunctionStoreFactory.Create());
}