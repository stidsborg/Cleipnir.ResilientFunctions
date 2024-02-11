using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobMessageStore : IMessageStore
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobMessageStore(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public Task Initialize() => Task.CompletedTask;

    public Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
        => AppendMessages(functionId, storedMessages: new[] { storedMessage });

    public Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null)
        => AppendMessage(functionId, new StoredMessage(messageJson, messageType, idempotencyKey));

    public Task<bool> ReplaceMessage(FunctionId functionId, int position, StoredMessage storedMessage)
        => throw new InvalidOperationException("AzureBlobMessageStore does not support replacing existing messages");

    private async Task<FunctionStatus> AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages)
    {
        var marshalledString = SimpleMarshaller.Serialize(storedMessages
            .SelectMany(storedMessage => new[] { storedMessage.MessageJson, storedMessage.MessageType, storedMessage.IdempotencyKey })
            .ToArray()
        );
        
        return await AppendOrCreate(functionId, marshalledString);
    }
    
    public async Task Truncate(FunctionId functionId)
    {
        var blobName = functionId.GetMessagesBlobName();
        
        await _blobContainerClient
            .GetAppendBlobClient(blobName)
            .DeleteIfExistsAsync();
    }
    
    public Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId)
        => InnerGetMessages(functionId, offset: 0)
            .SelectAsync(fetchedMessages => (IEnumerable<StoredMessage>) fetchedMessages.Messages);

    internal async Task<FetchedMessages> InnerGetMessages(
        FunctionId functionId, 
        int offset, 
        HashSet<string>? idempotencyKeys = null,
        string? leaseId = null
    )
    {
        var blobName = functionId.GetMessagesBlobName();
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
                return new FetchedMessages(Messages: ArraySegment<StoredMessage>.Empty, NewOffset: offset, ETag: null);
            if (e is { ErrorCode: "BlobNotFound", Status: 404 })
            {
                await appendBlobClient.CreateIfNotExistsAsync();
                return new FetchedMessages(Messages: ArraySegment<StoredMessage>.Empty, NewOffset: offset, ETag: null);                
            }

            throw;
        }
        
        var content = response.Value.Content.ToString();
        var messages = SimpleMarshaller.Deserialize(content);

        var storedMessages = new List<StoredMessage>(messages.Count / 3);
        for (var i = 0; i < messages.Count; i += 3)
        {
            var json = messages[i];
            var type = messages[i + 1];
            var idempotencyKey = messages[i + 2];
            var storedMessage = new StoredMessage(json!, type!, idempotencyKey);
            
            if (idempotencyKey != null && idempotencyKeys.Contains(idempotencyKey))
                continue;
            if (idempotencyKey != null)
                idempotencyKeys.Add(idempotencyKey);
            
            storedMessages.Add(storedMessage);
        }

        return new FetchedMessages(
            storedMessages, 
            NewOffset: offset + response.GetRawResponse().Headers.ContentLength!.Value,
            ETag: response.GetRawResponse().Headers.ETag
        );
    }

    public MessagesSubscription SubscribeToMessages(FunctionId functionId)
    {
        var sync = new object();
        var offset = 0;
        var disposed = false;
        var idempotencyKeys = new HashSet<string>();
        
        var subscription = new MessagesSubscription(
            pullNewMessages: async () =>
            {
                lock (sync)
                    if (disposed)
                        return ArraySegment<StoredMessage>.Empty;
                
                var (messages, newOffset, _) = await InnerGetMessages(functionId, offset, idempotencyKeys);
                offset = newOffset;
                return messages;
            },
            dispose: () =>
            {
                lock (sync)
                    disposed = true;

                return ValueTask.CompletedTask;
            }
        );

        return subscription;
    }

    private async Task<FunctionStatus> AppendOrCreate(FunctionId functionId, string marshalledString)
    {
        var messagesBlobName = functionId.GetMessagesBlobName();
        var messagesBlobClient = _blobContainerClient.GetAppendBlobClient(messagesBlobName);

        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(marshalledString));
        try
        {
            await messagesBlobClient.AppendBlockAsync(ms);
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") throw;
            await messagesBlobClient.CreateIfNotExistsAsync();
            await AppendOrCreate(functionId, marshalledString);
        }
        
        try
        {
            var blobName = functionId.GetStateBlobName();
            var blobClient = _blobContainerClient.GetBlobClient(blobName);

            RfTags rfTags;
            try
            {
                var blobTags = await blobClient.GetTagsAsync();
                rfTags = RfTags.ConvertFrom(blobTags.Value.Tags);
                return new FunctionStatus(rfTags.Status, rfTags.Epoch);
            }
            catch (RequestFailedException exception)
            {
                if (exception.Status == 404) throw new ConcurrentModificationException(functionId, exception);
                throw;
            }
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobNotFound") throw;
            throw new ConcurrentModificationException(functionId, e);
        }
    }
    
    internal readonly record struct FetchedMessages(IReadOnlyList<StoredMessage> Messages, int NewOffset, ETag? ETag);
}