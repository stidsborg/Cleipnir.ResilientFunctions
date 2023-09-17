using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

public static class FunctionStoreFactory
{
    public static BlobContainerClient BlobContainerClient { get; } = null!;
    public static AzureBlobFunctionStore FunctionStore { get; } = null!;
    public static Task<IFunctionStore> FunctionStoreTask => FunctionStore.CastTo<IFunctionStore>().ToTask();
    public static Task<IEventStore> EventStoreTask => FunctionStore.EventStore.ToTask();
    private static bool ShouldRun => Settings.ConnectionString != null;

    public static Task<IFunctionStore> Create() => FunctionStoreTask;
    
    static FunctionStoreFactory()
    {
        if (!ShouldRun) return;
        
        var blobServiceClient = new BlobServiceClient(Settings.ConnectionString);
        FunctionStore = CreateAndInitialize(prefix: nameof(StoreTests).ToLower()).GetAwaiter().GetResult();
        BlobContainerClient = blobServiceClient.GetBlobContainerClient(FunctionStore.ContainerName);
        
        BlobContainerClient.DeleteAllBlobs().GetAwaiter().GetResult();
    }
    
    public static async Task<AzureBlobFunctionStore> CreateAndInitialize(string prefix)
    {
        var functionStore = new AzureBlobFunctionStore(Settings.ConnectionString!, prefix);
        await functionStore.Initialize();

        return functionStore;
    }   
}