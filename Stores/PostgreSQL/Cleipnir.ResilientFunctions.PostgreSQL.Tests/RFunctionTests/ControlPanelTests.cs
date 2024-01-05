using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReInvokingExistingActionFromControlPanelSucceeds()
        => ReInvokingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseIsUpdatedForExecutingFunc()
        => LeaseIsUpdatedForExecutingFunc(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task LeaseIsUpdatedForExecutingAction()
        => LeaseIsUpdatedForExecutingAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents()
        => ControlPanelsExistingEventsContainsPreviouslyAddedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventsCanBeReplacedUsingControlPanel()
        => ExistingEventsCanBeReplacedUsingControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSave()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSave(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingEventsCanBeReplaced()
        => ExistingEventsCanBeReplaced(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingActivityCanBeReplacedWithValue()
        => ExistingActivityCanBeReplacedWithValue(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingActivityCanBeRemoved()
        => ExistingActivityCanBeRemoved(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ActivitiesAreUpdatedAfterRefresh()
        => ActivitiesAreUpdatedAfterRefresh(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingActivityCanBeSetToFailed()
        => ExistingActivityCanBeSetToFailed(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingActivityCanBeReplaced()
        => ExistingActivityCanBeReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ActivityCanBeStarted()
        => ActivityCanBeStarted(FunctionStoreFactory.Create());
}