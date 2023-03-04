using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
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
        => InnerGetEvents(functionId, offset: 0)
            .SelectAsync(fetchedEvents => (IEnumerable<StoredEvent>) fetchedEvents.Events);
    
    private async Task<FetchedEvents> InnerGetEvents(FunctionId functionId, int offset)
    {
        var blobName = $"{functionId}_events";
        var appendBlobClient = _blobContainerClient.GetAppendBlobClient(blobName);

        Response<BlobDownloadResult> response;
        try
        {
            response = await appendBlobClient.DownloadContentAsync(
                new BlobDownloadOptions { Range = new HttpRange(offset) }
            );
        }
        catch (RequestFailedException e)
        {
            if (e is { ErrorCode: "InvalidRange", Status: 416 })
                return new FetchedEvents(Events: ArraySegment<StoredEvent>.Empty, NewOffset: offset);
            if (e is { ErrorCode: "BlobNotFound", Status: 404 })
                return new FetchedEvents(Events: ArraySegment<StoredEvent>.Empty, NewOffset: offset);
            
            throw;
        }
        
        var content = response.Value.Content.ToString();
        var events = SimpleMarshaller.Deserialize(content);

        var storedEvents = new List<StoredEvent>(events.Count / 3);
        for (var i = 0; i < events.Count; i += 3)
        {
            var json = events[i];
            var type = events[i + 1];
            var idempotencyKey = events[i + 2];
            var storedEvent = new StoredEvent(json!, type!, idempotencyKey);
            storedEvents.Add(storedEvent);
        }

        return new FetchedEvents(storedEvents, NewOffset: offset + response.GetRawResponse().Headers.ContentLength!.Value);
    }

    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        var sync = new object();
        var offset = 0;
        var disposed = false;
        
        var subscription = new EventsSubscription(
            pullEvents: async () =>
            {
                lock (sync)
                    if (disposed)
                        return ArraySegment<StoredEvent>.Empty;
                
                var (events, newOffset) = await InnerGetEvents(functionId, offset);
                offset = newOffset;
                return events;
            },
            dispose: () =>
            {
                lock (sync)
                    disposed = true;

                return ValueTask.CompletedTask;
            }
        );

        return Task.FromResult(subscription);
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

    private readonly record struct FetchedEvents(IReadOnlyList<StoredEvent> Events, int NewOffset);
}