using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class StoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.StoreTests
{
    private static readonly BlobContainerClient BlobContainerClient = null!;
    private static readonly AzureBlobFunctionStore FunctionStore = null!;
    private static bool ShouldRun => Settings.ConnectionString != null; 
    
    static StoreTests()
    {
        if (!ShouldRun) return;
        
        var blobServiceClient = new BlobServiceClient(Settings.ConnectionString);
        FunctionStore = FunctionStoreFactory.CreateAndInitialize(prefix: nameof(StoreTests).ToLower()).GetAwaiter().GetResult();
        BlobContainerClient = blobServiceClient.GetBlobContainerClient(FunctionStore.ContainerName);
        
        BlobContainerClient.DeleteAllBlobs().GetAwaiter().GetResult();
    }
    
    [TestMethod]
    public override Task SunshineScenarioTest() => 
        SunshineScenarioTest(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenAsExpected()
        => SignOfLifeIsUpdatedWhenAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected()
        => SignOfLifeIsNotUpdatedWhenNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected()
        => BecomeLeaderSucceedsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderFailsWhenEpochIsNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task CreatingTheSameFunctionTwiceReturnsFalse()
        => CreatingTheSameFunctionTwiceReturnsFalse(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut()
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponeFunctionFailsWhenEpochIsNotAsExpected()
        => PostponeFunctionFailsWhenEpochIsNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
        => CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(
            FunctionStore.CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task OnlyEligibleCrashedFunctionsAreReturnedFromStore()
        => OnlyEligibleCrashedFunctionsAreReturnedFromStore(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task IncrementEpochSucceedsWhenEpochIsAsExpected()
        => IncrementEpochSucceedsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task IncrementEpochFailsWhenEpochIsNotAsExpected()
        => IncrementEpochFailsWhenEpochIsNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected()
        => SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected()
        => SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task DeletingExistingFunctionSucceeds()
        => DeletingExistingFunctionSucceeds(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task DeletingExistingFunctionFailsWhenEpochIsNotAsExpected()
        => DeletingExistingFunctionFailsWhenEpochIsNotAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task FailFunctionSucceedsWhenEpochIsAsExpected()
        => FailFunctionSucceedsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SetFunctionStateSucceedsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected()
        => SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExecutingFunctionCanBeSuspendedSuccessfully()
        => ExecutingFunctionCanBeSuspendedSuccessfully(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch()
        => RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated()
        => EventsCanBeFetchedAfterFunctionWithInitialEventsHasBeenCreated(FunctionStore.CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionStatusAndEpochCanBeSuccessfullyFetched()
        => FunctionStatusAndEpochCanBeSuccessfullyFetched(FunctionStore.CastTo<IFunctionStore>().ToTask());
    
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
    public override Task FunctionIsPostponedOnSuspensionAndEventCountMismatch()
        => FunctionIsPostponedOnSuspensionAndEventCountMismatch(FunctionStoreFactory.Create());
}