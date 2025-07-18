﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class StoreTests : TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NullParamScenarioTest()
        => NullParamScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseIsUpdatedWhenAsExpected() 
        => LeaseIsUpdatedWhenAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseIsNotUpdatedWhenNotAsExpected()
        => LeaseIsNotUpdatedWhenNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected()
        => BecomeLeaderSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CreatingTheSameFunctionTwiceReturnsFalse()
        => CreatingTheSameFunctionTwiceReturnsFalse(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction()
        => FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut()
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task PostponeFunctionFailsWhenEpochIsNotAsExpected()
        => PostponeFunctionFailsWhenEpochIsNotAsExpected(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
        => CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task OnlyEligibleCrashedFunctionsAreReturnedFromStore()
        => OnlyEligibleCrashedFunctionsAreReturnedFromStore(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task IncrementEpochSucceedsWhenEpochIsAsExpected()
        => IncrementEpochSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task IncrementEpochFailsWhenEpochIsNotAsExpected()
        => IncrementEpochFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeletingExistingFunctionSucceeds()
        => DeletingExistingFunctionSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FailFunctionSucceedsWhenEpochIsAsExpected()
        => FailFunctionSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SetFunctionStateSucceedsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExecutingFunctionCanBeSuspendedSuccessfully()
        => ExecutingFunctionCanBeSuspendedSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionStatusForNonExistingFunctionIsNull()
        => FunctionStatusForNonExistingFunctionIsNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch()
        => RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RestartingFunctionShouldSetInterruptedToFalse()
        => RestartingFunctionShouldSetInterruptedToFalse(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated()
        => MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionStatusAndEpochCanBeSuccessfullyFetched()
        => FunctionStatusAndEpochCanBeSuccessfullyFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EpochIsNotIncrementedOnCompletion()
        => EpochIsNotIncrementedOnCompletion(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EpochIsNotIncrementedOnPostponed()
        => EpochIsNotIncrementedOnPostponed(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EpochIsNotIncrementedOnFailure()
        => EpochIsNotIncrementedOnFailure(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EpochIsNotIncrementedOnSuspension()
        => EpochIsNotIncrementedOnSuspension(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction()
        => SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch()
        => FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptCountCanBeIncrementedForExecutingFunction()
        => InterruptCountCanBeIncrementedForExecutingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptCountCannotBeIncrementedForNonExecutingFunction()
        => InterruptCountCannotBeIncrementedForNonExecutingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptCountForNonExistingFunctionIsNull()
        => InterruptCountForNonExistingFunctionIsNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DefaultStateCanSetAndFetchedAfterwards()
        => DefaultStateCanSetAndFetchedAfterwards(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SucceededFunctionsCanBeFetchedSuccessfully()
        => SucceededFunctionsCanBeFetchedSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BulkScheduleInsertsAllFunctionsSuccessfully()
        => BulkScheduleInsertsAllFunctionsSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DifferentTypesAreFetchedByGetExpiredFunctionsCall()
        => DifferentTypesAreFetchedByGetExpiredFunctionsCall(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleInstancesCanBeFetchedForFlowType()
        => MultipleInstancesCanBeFetchedForFlowType(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TypeStoreSunshineScenarioTest()
        => TypeStoreSunshineScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FlowWithParentIsReturnedInSubsequentGetTest()
        => FlowWithParentIsReturnedInSubsequentGetTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest()
        => FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BatchOfLeasesCanBeUpdatedSimultaneously()
        => BatchOfLeasesCanBeUpdatedSimultaneously(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleFunctionsStatusCanBeFetched()
        => MultipleFunctionsStatusCanBeFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptedFunctionIsNotPostponedWhenFlagIsSet()
        => InterruptedFunctionIsNotPostponedWhenFlagIsSet(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptNothingWorks()
        => InterruptNothingWorks(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptedFunctionIsPostponedWhenIgnoringInterruptedFunction()
        => InterruptedFunctionIsPostponedWhenIgnoringInterruptedFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithMessagesAndEffects()
        => FunctionCanBeCreatedWithMessagesAndEffects(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithMessagesOnly()
        => FunctionCanBeCreatedWithMessagesOnly(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithEffectsOnly()
        => FunctionCanBeCreatedWithEffectsOnly(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RestartExecutionReturnsEffectsAndMessages()
        => RestartExecutionReturnsEffectsAndMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RestartExecutionWorksWithEmptyEffectsAndMessages()
        => RestartExecutionWorksWithEmptyEffectsAndMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionOwnedByReplicaIsPostponedAfterRescheduleFunctionsInvocation()
        => FunctionOwnedByReplicaIsPostponedAfterRescheduleFunctionsInvocation(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SuspensionSetsOwnerToNull()
        => SuspensionSetsOwnerToNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FailureSetsOwnerToNull()
        => FailureSetsOwnerToNull(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task PostponedSetsOwnerToNull()
        =>  PostponedSetsOwnerToNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SucceedSetsOwnerToNull()
        => SucceedSetsOwnerToNull(FunctionStoreFactory.Create());
}