using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

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
    public override Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages()
        => ControlPanelsExistingMessagesContainsPreviouslyAddedMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingMessagesCanBeReplacedUsingControlPanel()
        => ExistingMessagesCanBeReplacedUsingControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced()
        => ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave()
        => ConcurrentModificationOfExistingMessagesCausesExceptionOnSave(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced()
        => ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingMessagesCanBeReplaced()
        => ExistingMessagesCanBeReplaced(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingActivityCanBeReplacedWithValue()
        => ExistingActivityCanBeReplacedWithValue(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingActivityCanBeRemoved()
        => ExistingActivityCanBeRemoved(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ActivitiesAreUpdatedAfterRefresh()
        => ActivitiesAreUpdatedAfterRefresh(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(FunctionStoreFactory.Create());
    
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