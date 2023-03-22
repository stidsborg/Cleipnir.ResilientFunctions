using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobTimeoutStore : ITimeoutStore
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobTimeoutStore(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public Task Initialize() => Task.CompletedTask;

    public async Task UpsertTimeout(StoredTimeout storedTimeout)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        var blobName = GetTimeoutBlobName(functionId, timeoutId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        await blobClient.UploadAsync(
            content: new BinaryData(expiry.ToString()),
            options: new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>
                {
                    { "FunctionType", functionId.TypeId.Value }, 
                    { "TimeoutExpires", expiry.ToString() }
                } 
            }
        );
    }

    public async Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        var blobName = GetTimeoutBlobName(functionId, timeoutId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        await blobClient.DeleteIfExistsAsync();
    }

    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        var timeoutBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId}' AND TimeoutExpires <= '{expiresBefore}'"
        );

        var storedTimeoutTasks = new List<Task<StoredTimeout?>>();
        await foreach (var timeoutBlob in timeoutBlobs)
            storedTimeoutTasks.Add(FetchExpiryAndConvertToStoredTimeout(timeoutBlob.BlobName));

        var storedTimeouts = new List<StoredTimeout>(capacity: storedTimeoutTasks.Count);
        foreach (var storedTimeoutTask in storedTimeoutTasks)
        {
            var storedTimeout = await storedTimeoutTask;
            if (storedTimeout != null)
                storedTimeouts.Add(storedTimeout);
        }
        
        return storedTimeouts;
    }

    private async Task<StoredTimeout?> FetchExpiryAndConvertToStoredTimeout(string blobName)
    {
        var (functionId, timeoutId) = GetFunctionAndTimeoutIdFromBlobName(blobName);

        try
        {
            var content = await _blobContainerClient
                .GetBlobClient(blobName)
                .DownloadContentAsync();

            var storedTimeout = new StoredTimeout(
                functionId,
                timeoutId,
                Expiry: long.Parse(content.Value.Content.ToString())
            );
            return storedTimeout;
        }  catch (RequestFailedException e)
        {
            if (e is { ErrorCode: "BlobNotFound", Status: 404 })
                return null;
            
            throw;
        }
    }

    private static string GetTimeoutBlobName(FunctionId functionId, string timeoutId)
    {
        functionId.Validate();
        var (functionTypeId, functionInstanceId) = functionId;
        return $"timeout¤{functionTypeId}¤{functionInstanceId}¤{timeoutId}";
    }

    private static FunctionAndTimeoutId GetFunctionAndTimeoutIdFromBlobName(string blobName)
    {
        var split = blobName.Split("¤");
        return new FunctionAndTimeoutId(new FunctionId(split[1], split[2]), split[3]);
    }

    private record FunctionAndTimeoutId(FunctionId FunctionId, string TimeoutId);
}