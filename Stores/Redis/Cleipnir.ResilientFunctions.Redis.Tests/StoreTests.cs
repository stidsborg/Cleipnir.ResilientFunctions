namespace Cleipnir.ResilientFunctions.Redis.Tests;

[TestClass]
public class StoreTests : ResilientFunctions.Tests.TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenAsExpected() 
        => SignOfLifeIsUpdatedWhenAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected() 
        => SignOfLifeIsNotUpdatedWhenNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected() 
        => BecomeLeaderSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task CreatingTheSameFunctionTwiceReturnsFalse()
        => CreatingTheSameFunctionTwiceReturnsFalse(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut()
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PostponeFunctionFailsWhenEpochIsNotAsExpected()
        => PostponeFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
        => CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task OnlyEligibleCrashedFunctionsAreReturnedFromStore()
        => OnlyEligibleCrashedFunctionsAreReturnedFromStore(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task IncrementEpochSucceedsWhenEpochIsAsExpected()
        => IncrementEpochSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task IncrementEpochFailsWhenEpochIsNotAsExpected()
        => IncrementEpochFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected()
        => SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected()
        => SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task DeletingExistingFunctionSucceeds()
        => DeletingExistingFunctionSucceeds(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task DeletingExistingFunctionFailsWhenEpochIsNotAsExpected()
        => DeletingExistingFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task FailFunctionSucceedsWhenEpochIsAsExpected()
        => FailFunctionSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SetFunctionStateSucceedsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExecutingFunctionCanBeSuspendedSuccessfully()
        => ExecutingFunctionCanBeSuspendedSuccessfully(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch()
        => RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated()
        => EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task FunctionStatusAndEpochCanBeSuccessfullyFetched()
        => FunctionStatusAndEpochCanBeSuccessfullyFetched(FunctionStoreFactory.FunctionStoreTask);
}