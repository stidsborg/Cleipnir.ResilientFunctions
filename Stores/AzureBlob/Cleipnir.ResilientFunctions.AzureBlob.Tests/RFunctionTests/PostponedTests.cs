using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class PostponedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task PostponedActionIsCompletedAfterInMemoryTimeout()
        => PostponedActionIsCompletedAfterInMemoryTimeout(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedAction()
        => ThrownPostponeExceptionResultsInPostponedAction(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedActionWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedActionWithScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFunc()
        => ThrownPostponeExceptionResultsInPostponedFunc(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook()
        => ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp()
        => ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task ScheduleAtActionIsCompletedAfterDelay()
        => ScheduleAtActionIsCompletedAfterDelay(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ScheduleAtFuncIsCompletedAfterDelay()
        => ScheduleAtFuncIsCompletedAfterDelay(FunctionStoreFactory.FunctionStoreTask);
}