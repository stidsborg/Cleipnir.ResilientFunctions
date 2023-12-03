using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobActivityStore : IActivityStore
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobActivityStore(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public Task Initialize() => Task.CompletedTask;

    public async Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity)
    {
        var blobName = functionId.GetActivityBlobName(storedActivity.ActivityId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredActivity)}.{nameof(StoredActivity.ActivityId)}", storedActivity.ActivityId },
                { $"{nameof(StoredActivity)}.{nameof(StoredActivity.Result)}", storedActivity.Result },
                { $"{nameof(StoredActivity)}.{nameof(StoredActivity.StoredException)}", JsonSerializer.Serialize(storedActivity.StoredException) },
                { $"{nameof(StoredActivity)}.{nameof(StoredActivity.WorkStatus)}", ((int)storedActivity.WorkStatus).ToString() }
            }
        );

        await blobClient.UploadAsync(new BinaryData(content), overwrite: true);
    }

    public async Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId)
    {
        var activityBlobs = _blobContainerClient
            .GetBlobsAsync(prefix: functionId.GetActivityBlobName(activityId: ""));
        
        var storedActivities = new List<StoredActivity>();
        await foreach (var activityBlob in activityBlobs)
        {
            var blobClient = _blobContainerClient.GetBlobClient(activityBlob.Name);
            var contentResponse = await blobClient.DownloadContentAsync();
            var content = contentResponse.Value.Content.ToString();
            var dictionary = SimpleDictionaryMarshaller.Deserialize(content, expectedCount: 4);
            var activityId = dictionary[$"{nameof(StoredActivity)}.{nameof(StoredActivity.ActivityId)}"];
            var result = dictionary[$"{nameof(StoredActivity)}.{nameof(StoredActivity.Result)}"];
            var storedException = 
                dictionary[$"{nameof(StoredActivity)}.{nameof(StoredActivity.StoredException)}"] == null
                ? null
                : JsonSerializer.Deserialize<StoredException>(dictionary[$"{nameof(StoredActivity)}.{nameof(StoredActivity.StoredException)}"]!);
            var workStatus = (WorkStatus) int.Parse(dictionary[$"{nameof(StoredActivity)}.{nameof(StoredActivity.WorkStatus)}"]!);
            storedActivities.Add(new StoredActivity(activityId!, workStatus, result, storedException));
        }

        return storedActivities;
    }

    public async Task DeleteActivityResult(FunctionId functionId, string activityId)
    {
        var activityBlobName = functionId.GetActivityBlobName(activityId);
        var blobClient = _blobContainerClient.GetBlobClient(activityBlobName);
        try
        {
            await blobClient.DeleteAsync();
        }
        catch (RequestFailedException exception)
        {
            if (exception.Status != 404) throw;
        }
        
    }
}