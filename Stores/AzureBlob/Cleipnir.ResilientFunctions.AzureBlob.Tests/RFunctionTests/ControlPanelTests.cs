using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ReInvokingExistingActionFromControlPanelSucceeds()
        => ReInvokingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task LastSignOfLifeIsUpdatedForExecutingFunc()
        => LastSignOfLifeIsUpdatedForExecutingFunc(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task LastSignOfLifeIsUpdatedForExecutingAction()
        => LastSignOfLifeIsUpdatedForExecutingAction(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents()
        => ControlPanelsExistingEventsContainsPreviouslyAddedEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExistingEventsCanBeReplacedUsingControlPanel()
        => ExistingEventsCanBeReplacedUsingControlPanel(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSucceed()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSucceed(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task ExistingEventsCanBeReplaced()
        => ExistingEventsCanBeReplaced(FunctionStoreFactory.FunctionStoreTask);
}