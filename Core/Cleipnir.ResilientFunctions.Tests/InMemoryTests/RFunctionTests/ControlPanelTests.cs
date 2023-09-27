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
    public override Task LastSignOfLifeIsUpdatedForExecutingFunc()
        => LastSignOfLifeIsUpdatedForExecutingFunc(Utils.CreateInMemoryFunctionStoreTask());
    
    [TestMethod]
    public override Task LastSignOfLifeIsUpdatedForExecutingAction()
        => LastSignOfLifeIsUpdatedForExecutingAction(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents()
        => ControlPanelsExistingEventsContainsPreviouslyAddedEvents(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingEventsCanBeReplacedUsingControlPanel()
        => ExistingEventsCanBeReplacedUsingControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSave()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSave(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingEventsCanBeReplaced()
        => ExistingEventsCanBeReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(Utils.CreateInMemoryFunctionStoreTask());
}