using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokingExistingActionFromControlPanelSucceeds()
        => ReInvokingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task LeaseIsUpdatedForExecutingFunc()
        => LeaseIsUpdatedForExecutingFunc(Utils.CreateInMemoryFunctionStoreTask());
    
    [TestMethod]
    public override Task LeaseIsUpdatedForExecutingAction()
        => LeaseIsUpdatedForExecutingAction(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages()
        => ControlPanelsExistingMessagesContainsPreviouslyAddedMessages(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingMessagesCanBeReplacedUsingControlPanel()
        => ExistingMessagesCanBeReplacedUsingControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced()
        => ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave()
        => ConcurrentModificationOfExistingMessagesCausesExceptionOnSave(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced()
        => ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingMessagesCanBeReplaced()
        => ExistingMessagesCanBeReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingActivityCanBeReplacedWithValue()
        => ExistingActivityCanBeReplacedWithValue(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ActivityCanBeStarted()
        => ActivityCanBeStarted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingActivityCanBeReplaced()
        => ExistingActivityCanBeReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingActivityCanBeRemoved()
        => ExistingActivityCanBeRemoved(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ActivitiesAreUpdatedAfterRefresh()
        => ActivitiesAreUpdatedAfterRefresh(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingActivityCanBeSetToFailed()
        => ExistingActivityCanBeSetToFailed(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(Utils.CreateInMemoryFunctionStoreTask());
}