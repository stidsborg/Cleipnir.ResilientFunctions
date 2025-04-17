using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ControlPanelTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(FunctionStoreFactory.Create());

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
    public override Task SucceedingExistingParamlessFromControlPanelSucceeds()
        => SucceedingExistingParamlessFromControlPanelSucceeds(FunctionStoreFactory.Create());

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
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState(FunctionStoreFactory.Create());

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
    public override Task ExistingEffectCanBeReplacedWithValue()
        => ExistingEffectCanBeReplacedWithValue(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingStateCanBeReplacedRemovedAndAdded()
        => ExistingStateCanBeReplacedRemovedAndAdded(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingTimeoutCanBeUpdatedForAction()
        => ExistingTimeoutCanBeUpdatedForAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingTimeoutCanBeUpdatedForFunc()
        => ExistingTimeoutCanBeUpdatedForFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEffectCanBeRemoved()
        => ExistingEffectCanBeRemoved(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsAreOnlyFetchedOnPropertyInvocation()
        => EffectsAreOnlyFetchedOnPropertyInvocation(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsAreCachedAfterInitialFetch()
        => EffectsAreCachedAfterInitialFetch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsAreUpdatedAfterRefresh()
        => EffectsAreUpdatedAfterRefresh(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEffectCanBeSetToFailed()
        => ExistingEffectCanBeSetToFailed(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectRawBytesResultCanFetched()
        => EffectRawBytesResultCanFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEffectCanBeReplaced()
        => ExistingEffectCanBeReplaced(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectCanBeStarted()
        => EffectCanBeStarted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CorrelationsCanBeChanged()
        => CorrelationsCanBeChanged(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DeleteRemovesFunctionFromAllStores()
        => DeleteRemovesFunctionFromAllStores(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ClearFailedEffectsRemovesFailedEffectBeforeRestart()
        => ClearFailedEffectsRemovesFailedEffectBeforeRestart(FunctionStoreFactory.Create());
}