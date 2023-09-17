﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class StoreTests : TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenAsExpected() 
        => SignOfLifeIsUpdatedWhenAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected()
        => SignOfLifeIsNotUpdatedWhenNotAsExpected(FunctionStoreFactory.Create());

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
    public override Task SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected()
        => SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected()
        => SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

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
    public override Task SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExecutingFunctionCanBeSuspendedSuccessfully()
        => ExecutingFunctionCanBeSuspendedSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch()
        => RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated()
        => EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionStatusAndEpochCanBeSuccessfullyFetched()
        => FunctionStatusAndEpochCanBeSuccessfullyFetched(FunctionStoreFactory.Create());
}