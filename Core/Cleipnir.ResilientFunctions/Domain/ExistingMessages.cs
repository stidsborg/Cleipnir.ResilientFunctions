using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingMessages 
{
    private readonly StoredId _storedId;
    private List<MessageAndIdempotencyKey>? _receivedMessages;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;

    public Task<IReadOnlyList<MessageAndIdempotencyKey>> MessagesWithIdempotencyKeys => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<MessageAndIdempotencyKey>) t.Result.ToList());
    public Task<IReadOnlyList<object>> AsObjects => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<object>) t.Result.Select(m => m.Message).ToList());
    public Task<int> Count => GetReceivedMessages().SelectAsync(messages => messages.Count);

    public ExistingMessages(StoredId storedId, IMessageStore messageStore, ISerializer serializer)
    {
        _storedId = storedId;
        _messageStore = messageStore;
        _serializer = serializer;
    }

    private async Task<List<MessageAndIdempotencyKey>> GetReceivedMessages()
    {
        if (_receivedMessages is not null)
            return _receivedMessages;
        
        var storedMessages = await _messageStore.GetMessages(_storedId, skip: 0);
        return _receivedMessages = storedMessages
            .Select(m =>
                new MessageAndIdempotencyKey(
                    _serializer.DeserializeMessage(m.StoredMessage.MessageContent, m.StoredMessage.MessageType),
                    m.StoredMessage.IdempotencyKey
                )
            ).ToList();
    }
    
    public async Task Clear()
    {
        var receivedMessages = await GetReceivedMessages();
        await _messageStore.Truncate(_storedId);
        receivedMessages.Clear();  
    }

    public async Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
    {
        var receivedMessages = await GetReceivedMessages(); 
        var (json, type) = _serializer.SerializeMessage(message, typeof(T));
        await _messageStore.AppendMessage(
            _storedId, new StoredMessage(json, type, idempotencyKey)
        );
        
        receivedMessages.Add(new MessageAndIdempotencyKey(message, idempotencyKey));  
    } 
    
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        var receivedMessages = await GetReceivedMessages();
        if (position >= receivedMessages.Count)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{receivedMessages.Count}'", nameof(position));
        
        var (json, type) = _serializer.SerializeMessage(message, typeof(T));
        await _messageStore.ReplaceMessage(_storedId, position, new StoredMessage(json, type, idempotencyKey));
        
        receivedMessages[position] = new MessageAndIdempotencyKey(message, idempotencyKey);  
    }

    /// <summary>
    /// Removes the message at the provided position.
    /// A removed message is replaced with a NoOp message in order to preserve other positions. 
    /// </summary>
    /// <param name="position">Message position</param>
    public Task Remove(int position) => Replace(position, NoOp.Instance);

    /// <summary>
    /// Remove all existing fires timeouts from messages
    /// </summary>
    public async Task RemoveTimeouts()
    {
        var receivedMessages = await GetReceivedMessages();
        for (var i = 0; i < receivedMessages.Count; i++)
        {
            if (receivedMessages[i].Message is TimeoutEvent)
                await Remove(i);
        }
    }
}