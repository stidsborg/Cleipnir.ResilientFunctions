using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class StoreTests : ResilientFunctions.Tests.TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.Create());

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
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponeFunctionFailsWhenEpochIsNotAsExpected()
        => PostponeFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(FunctionStoreFactory.Create());
    
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
    public override Task SaveStateOfExecutingFunctionSucceedsWhenEpochIsAsExpected()
        => SaveStateOfExecutingFunctionSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SaveStateOfExecutingFunctionFailsWhenEpochIsNotAsExpected()
        => SaveStateOfExecutingFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DeletingExistingFunctionSucceeds()
        => DeletingExistingFunctionSucceeds(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DeletingExistingFunctionFailsWhenEpochIsNotAsExpected()
        => DeletingExistingFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());
    
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
    public override Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch()
        => RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(FunctionStoreFactory.Create());
    
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
    public override Task FunctionIsPostponedOnSuspensionAndMessageCountMismatch()
        => FunctionIsPostponedOnSuspensionAndMessageCountMismatch(FunctionStoreFactory.Create());
}