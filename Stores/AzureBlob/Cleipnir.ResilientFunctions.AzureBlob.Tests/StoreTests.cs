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

    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
    {
        throw new System.NotImplementedException();
    }

    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
    {
        throw new System.NotImplementedException();
    }

    public override Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency()
    {
        throw new System.NotImplementedException();
    }

    public override Task IncrementEpochSucceedsWhenEpochIsAsExpected()
    {
        throw new System.NotImplementedException();
    }

    public override Task IncrementEpochFailsWhenEpochIsNotAsExpected()
    {
        throw new System.NotImplementedException();
    }
}