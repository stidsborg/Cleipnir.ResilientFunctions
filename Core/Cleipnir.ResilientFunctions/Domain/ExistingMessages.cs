using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingMessages : IEnumerable<object>
{
    private readonly FunctionId _functionId;
    private readonly List<MessageAndIdempotencyKey> _receivedMessages;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    public IReadOnlyList<MessageAndIdempotencyKey> MessagesWithIdempotencyKeys => _receivedMessages.ToList();
    
    public ExistingMessages(FunctionId functionId, List<MessageAndIdempotencyKey> receivedMessages, IMessageStore messageStore, ISerializer serializer)
    {
        _functionId = functionId;
        _receivedMessages = receivedMessages;
        _messageStore = messageStore;
        _serializer = serializer;
    }

    public object this[int index] => _receivedMessages[index].Message;

    public async Task Clear()
    {
        await _messageStore.Truncate(_functionId);
        _receivedMessages.Clear();  
    }

    public async Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
    {
        var (json, type) = _serializer.SerializeMessage(message);
        await _messageStore.AppendMessage(
            _functionId, new StoredMessage(json, type, idempotencyKey)
        );
        
        _receivedMessages.Add(new MessageAndIdempotencyKey(message, idempotencyKey));  
    } 
    
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        if (position >= _receivedMessages.Count)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{_receivedMessages.Count}'", nameof(position));
        
        var (json, type) = _serializer.SerializeMessage(message);
        await _messageStore.ReplaceMessage(_functionId, position, new StoredMessage(json, type, idempotencyKey));
        
        _receivedMessages.Add(new MessageAndIdempotencyKey(message, idempotencyKey));  
    }

    public IEnumerator<object> GetEnumerator() => _receivedMessages.Select(e => e.Message).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}