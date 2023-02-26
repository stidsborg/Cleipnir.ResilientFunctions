using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobEventStore : IEventStore
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobEventStore(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public Task Initialize() => Task.CompletedTask;

    public Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
        => AppendEvents(functionId, storedEvents: new[] { storedEvent });

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var marshalledString = SimpleMarshaller.Serialize(storedEvents
            .SelectMany(storedEvent => new[] { storedEvent.EventJson, storedEvent.EventType, storedEvent.IdempotencyKey })
            .ToArray()
        );
        
        await AppendOrCreate(functionId, marshalledString);
    }

    public async Task Truncate(FunctionId functionId)
    {
        functionId.Validate();
        var blobName = functionId.ToString();
        
        await _blobContainerClient
            .GetAppendBlobClient(blobName)
            .DeleteIfExistsAsync();
    }

    public Task<bool> Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, int? expectedCount)
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
    {
        throw new NotImplementedException();
    }

    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        throw new NotImplementedException();
    }

    private async Task AppendOrCreate(FunctionId functionId, string marshalledString)
    {
        functionId.Validate();
        var blobName = $"{functionId}_events";
        var appendBlobClient = _blobContainerClient.GetAppendBlobClient(blobName);
        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(marshalledString));
        try
        {
            await appendBlobClient.AppendBlockAsync(ms);
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") throw;
            await appendBlobClient.CreateIfNotExistsAsync();
            await AppendOrCreate(functionId, marshalledString);
        }
    }
}