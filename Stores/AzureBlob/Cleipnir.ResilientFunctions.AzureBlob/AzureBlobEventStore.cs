using System.Collections.Immutable;
using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobEventStore : IEventStore
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobEventStore(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public Task Initialize() => Task.CompletedTask;

    public Task<SuspensionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent)
        => AppendEvents(functionId, storedEvents: new[] { storedEvent });

    public Task<SuspensionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task<SuspensionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var marshalledString = SimpleMarshaller.Serialize(storedEvents
            .SelectMany(storedEvent => new[] { storedEvent.EventJson, storedEvent.EventType, storedEvent.IdempotencyKey })
            .ToArray()
        );
        
        return await AppendOrCreate(functionId, marshalledString);
    }

    public async Task CreateEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var marshalledString = SimpleMarshaller.Serialize(storedEvents
            .SelectMany(storedEvent => new[] { storedEvent.EventJson, storedEvent.EventType, storedEvent.IdempotencyKey })
            .ToArray()
        );
        
        var eventsBlobName = functionId.GetEventsBlobName();
        var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);
        
        await eventsBlobClient.CreateAsync(
            new AppendBlobCreateOptions { Conditions = new AppendBlobRequestConditions { IfNoneMatch = new ETag("*") } }
        );
        
        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(marshalledString));
        await eventsBlobClient.AppendBlockAsync(ms);
    }

    public async Task Truncate(FunctionId functionId)
    {
        var blobName = functionId.GetEventsBlobName();
        
        await _blobContainerClient
            .GetAppendBlobClient(blobName)
            .DeleteIfExistsAsync();
    }

    internal async Task Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, string leaseId)
    {
        var blobName = functionId.GetEventsBlobName();
        var blobClient = _blobContainerClient.GetAppendBlobClient(blobName);
        await blobClient
            .CreateAsync(new AppendBlobCreateOptions
            {
                Conditions = new AppendBlobRequestConditions {LeaseId = leaseId}
            });
        
        var marshalledString = SimpleMarshaller.Serialize(storedEvents
            .SelectMany(storedEvent => new[] { storedEvent.EventJson, storedEvent.EventType, storedEvent.IdempotencyKey })
            .ToArray()
        );
        
        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(marshalledString));
        await blobClient.AppendBlockAsync(
            ms, 
            new AppendBlobAppendBlockOptions
            {
                Conditions = new AppendBlobRequestConditions {LeaseId = leaseId}
            });
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
        => InnerGetEvents(functionId, offset: 0)
            .SelectAsync(fetchedEvents => (IEnumerable<StoredEvent>) fetchedEvents.Events);

    internal async Task<FetchedEvents> InnerGetEvents(
        FunctionId functionId, 
        int offset, 
        HashSet<string>? idempotencyKeys = null,
        string? leaseId = null
    )
    {
        var blobName = functionId.GetEventsBlobName();
        var appendBlobClient = _blobContainerClient.GetAppendBlobClient(blobName);
        idempotencyKeys ??= new HashSet<string>();
        
        Response<BlobDownloadResult> response;
        try
        {
            var blobDownloadOptions = new BlobDownloadOptions { Range = new HttpRange(offset) };
            if (leaseId != null)
                blobDownloadOptions.Conditions = new BlobRequestConditions
                {
                    LeaseId = leaseId
                };
                    
            response = await appendBlobClient.DownloadContentAsync(blobDownloadOptions);
        }
        catch (RequestFailedException e)
        {
            if (e is { ErrorCode: "InvalidRange", Status: 416 })
                return new FetchedEvents(Events: ArraySegment<StoredEvent>.Empty, NewOffset: offset, ETag: null);
            if (e is { ErrorCode: "BlobNotFound", Status: 404 })
            {
                await appendBlobClient.CreateIfNotExistsAsync();
                return new FetchedEvents(Events: ArraySegment<StoredEvent>.Empty, NewOffset: offset, ETag: null);                
            }

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
            
            if (idempotencyKey != null && idempotencyKeys.Contains(idempotencyKey))
                continue;
            if (idempotencyKey != null)
                idempotencyKeys.Add(idempotencyKey);
            
            storedEvents.Add(storedEvent);
        }

        return new FetchedEvents(
            storedEvents, 
            NewOffset: offset + response.GetRawResponse().Headers.ContentLength!.Value,
            ETag: response.GetRawResponse().Headers.ETag
        );
    }

    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        var sync = new object();
        var offset = 0;
        var disposed = false;
        var idempotencyKeys = new HashSet<string>();
        
        var subscription = new EventsSubscription(
            pullEvents: async () =>
            {
                lock (sync)
                    if (disposed)
                        return ArraySegment<StoredEvent>.Empty;
                
                var (events, newOffset, _) = await InnerGetEvents(functionId, offset, idempotencyKeys);
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

    private async Task<SuspensionStatus> AppendOrCreate(FunctionId functionId, string marshalledString)
    {
        var eventsBlobName = functionId.GetEventsBlobName();
        var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);

        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(marshalledString));
        try
        {
            await eventsBlobClient.AppendBlockAsync(ms);
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") throw;
            await eventsBlobClient.CreateIfNotExistsAsync();
            await AppendOrCreate(functionId, marshalledString);
        }
        
        try
        {
            var tags = await eventsBlobClient.GetTagsAsync().SelectAsync(r => r.Value.Tags);
            var success = tags.TryGetValue("suspended_at_epoch", out var epochStr);
            if (!success)
                return new SuspensionStatus(Suspended: false, Epoch: default);

            var suspendedAtEpoch = int.Parse(epochStr!);
            return new SuspensionStatus(Suspended: true, suspendedAtEpoch);
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") throw;
            throw new ConcurrentModificationException(functionId);
        }
    }

    internal async Task<SuspensionResult> SetSuspendedAtEpochTag(FunctionId functionId, int expectedEpoch, int expectedEventCount)
    {
        var eventsBlobName = functionId.GetEventsBlobName();
        var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);
        
        var eventCount = await GetEvents(functionId).SelectAsync(e => e.Count());
        if (expectedEventCount != eventCount)
            return SuspensionResult.EventCountMismatch;
        
        var tagsResponse = await GetTags(functionId);
        
        var tags = tagsResponse!.Value.Tags;
        var suspendedAtEpoch = tags.ContainsKey("suspended_at_epoch")
            ? int.Parse(tags["suspended_at_epoch"])
            : default(int?);
        
        if (suspendedAtEpoch.HasValue && suspendedAtEpoch.Value > expectedEpoch) 
            return SuspensionResult.ConcurrentStateModification;
        
        try
        {
            await eventsBlobClient.SetTagsAsync(
                new Dictionary<string, string> { { "suspended_at_epoch", expectedEpoch.ToString() } },
                new BlobRequestConditions { IfMatch = tagsResponse.GetRawResponse().Headers.ETag }
            );
            return SuspensionResult.Success;
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") 
                return SuspensionResult.ConcurrentStateModification;
            
            await eventsBlobClient.CreateIfNotExistsAsync();
            return await SetSuspendedAtEpochTag(functionId, expectedEpoch, expectedEventCount);
        }
    }

    internal async Task ForceSetSuspendedAtEpochTag(FunctionId functionId, int epoch, string? leaseId)
    {
        var eventsBlobName = functionId.GetEventsBlobName();
        var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);
        try
        {
            await eventsBlobClient.SetTagsAsync(
                new Dictionary<string, string> { { "suspended_at_epoch", epoch.ToString() } },
                leaseId == null ? new BlobRequestConditions() : new BlobRequestConditions { LeaseId = leaseId }
            );
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound")
                throw;
            
            await eventsBlobClient.CreateIfNotExistsAsync();
            await ForceSetSuspendedAtEpochTag(functionId, epoch, leaseId);
        }
    }

    private async Task<Response<GetBlobTagResult>?> GetTags(FunctionId functionId)
    {
        var eventsBlobName = functionId.GetEventsBlobName();
        var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);
        
        return await eventsBlobClient.GetTagsAsync();
    }


    internal readonly record struct FetchedEvents(IReadOnlyList<StoredEvent> Events, int NewOffset, ETag? ETag);
}