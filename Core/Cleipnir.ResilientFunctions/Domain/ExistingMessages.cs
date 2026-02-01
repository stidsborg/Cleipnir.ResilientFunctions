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
    private List<StoredMessage>? _receivedMessages;
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
            return _receivedMessages.Select(m =>
                new MessageAndIdempotencyKey(
                    _serializer.Deserialize(m.MessageContent, _serializer.ResolveType(m.MessageType)!),
                    m.IdempotencyKey
                )
            ).ToList();

        var storedMessages = await _messageStore.GetMessages(_storedId);
        _receivedMessages = storedMessages.ToList();
        return await GetReceivedMessages();
    }
    
    public async Task Clear()
    {
        var receivedMessages = await GetReceivedMessages();
        await _messageStore.Truncate(_storedId);
        receivedMessages.Clear();  
    }

    public async Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
    {
        var json = _serializer.Serialize(message, message.GetType());
        var type = _serializer.SerializeType(message.GetType());
        await _messageStore.AppendMessage(
            _storedId, new StoredMessage(json, type, Position: 0, idempotencyKey)
        );

        // Invalidate cache so it will be re-fetched with correct positions
        _receivedMessages = null;
    } 
    
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        if (_receivedMessages is null)
            await GetReceivedMessages();

        var storedMessage = _receivedMessages!.OrderBy(m => m.Position).Skip(position).FirstOrDefault();
        if (storedMessage == null)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{_receivedMessages!.Count}'", nameof(position));

        var json = _serializer.Serialize(message, message.GetType());
        var type = _serializer.SerializeType(message.GetType());
        await _messageStore.ReplaceMessage(_storedId, storedMessage.Position, new StoredMessage(json, type, Position: storedMessage.Position, idempotencyKey));

        // Invalidate cache so it will be re-fetched with correct data
        _receivedMessages = null;
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