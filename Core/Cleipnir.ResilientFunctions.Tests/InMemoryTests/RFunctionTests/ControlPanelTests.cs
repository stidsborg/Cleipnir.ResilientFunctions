using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ControlPanelTests : TestTemplates.FunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());
    
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
    public override Task SucceedingExistingParamlessFromControlPanelSucceeds()
        => SucceedingExistingParamlessFromControlPanelSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());
    
    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(Utils.CreateInMemoryFunctionStoreTask());

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
    public override Task ExistingEffectCanBeReplacedWithValue()
        => ExistingEffectCanBeReplacedWithValue(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task EffectCanBeStarted()
        => EffectCanBeStarted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task EffectRawBytesResultCanFetched()
        => EffectRawBytesResultCanFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEffectCanBeReplaced()
        => ExistingEffectCanBeReplaced(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingEffectCanBeRemoved()
        => ExistingEffectCanBeRemoved(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task EffectsAreOnlyFetchedOnPropertyInvocation()
        => EffectsAreOnlyFetchedOnPropertyInvocation(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsAreCachedAfterInitialFetch()
        => EffectsAreCachedAfterInitialFetch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsAreUpdatedAfterRefresh()
        => EffectsAreUpdatedAfterRefresh(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingEffectCanBeSetToFailed()
        => ExistingEffectCanBeSetToFailed(Utils.CreateInMemoryFunctionStoreTask());
    
    [TestMethod]
    public override Task SaveChangesPersistsChangedResult()
        => SaveChangesPersistsChangedResult(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingTimeoutCanBeUpdatedForAction()
        => ExistingTimeoutCanBeUpdatedForAction(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingTimeoutCanBeUpdatedForFunc()
        => ExistingTimeoutCanBeUpdatedForFunc(Utils.CreateInMemoryFunctionStoreTask());

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