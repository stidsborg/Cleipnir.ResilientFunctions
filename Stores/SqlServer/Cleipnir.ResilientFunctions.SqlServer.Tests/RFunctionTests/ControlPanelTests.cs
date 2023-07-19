using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokingExistingActionFromControlPanelSucceeds()
        => ReInvokingExistingActionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task LastSignOfLifeIsUpdatedForExecutingFunc()
        => LastSignOfLifeIsUpdatedForExecutingFunc(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task LastSignOfLifeIsUpdatedForExecutingAction()
        => LastSignOfLifeIsUpdatedForExecutingAction(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents()
        => ControlPanelsExistingEventsContainsPreviouslyAddedEvents(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingEventsCanBeReplacedUsingControlPanel()
        => ExistingEventsCanBeReplacedUsingControlPanel(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation()
        => ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsCausesExceptionOnSucceed()
        => ConcurrentModificationOfExistingEventsCausesExceptionOnSucceed(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced()
        => ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced(Sql.AutoCreateAndInitializeStore());
}