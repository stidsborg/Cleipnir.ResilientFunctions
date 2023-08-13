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

    public async Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        var blobName = functionId.GetTimeoutBlobName(timeoutId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        try
        {
            await blobClient.UploadAsync(
                content: new BinaryData(expiry.ToString()),
                options: new BlobUploadOptions
                {
                    Tags = new Dictionary<string, string>
                    {
                        { "FunctionType", functionId.TypeId.Value },
                        { "TimeoutExpires", expiry.ToString() }
                    },
                    Conditions = overwrite ? null : new BlobRequestConditions { IfNoneMatch = new ETag("*") }
                }
            );
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode == "BlobAlreadyExists") return;
            
            throw;
        }
    }

    public async Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        var blobName = functionId.GetTimeoutBlobName(timeoutId);
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
        var (_, type, instance, timeoutId) = Utils.SplitIntoParts(blobName);
        var functionId = new FunctionId(type, instance);
        
        try
        {
            var content = await _blobContainerClient
                .GetBlobClient(blobName)
                .DownloadContentAsync();

            var storedTimeout = new StoredTimeout(
                functionId,
                timeoutId!,
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
}