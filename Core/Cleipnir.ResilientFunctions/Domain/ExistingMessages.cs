using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingMessages 
{
    private readonly FlowId _flowId;
    private List<MessageAndIdempotencyKey>? _receivedMessages;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;

    public Task<IReadOnlyList<MessageAndIdempotencyKey>> MessagesWithIdempotencyKeys => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<MessageAndIdempotencyKey>) t.Result.ToList());
    public Task<IReadOnlyList<object>> AsObjects => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<object>) t.Result.Select(m => m.Message).ToList());
    public Task<int> Count => GetReceivedMessages().SelectAsync(messages => messages.Count);

    public ExistingMessages(FlowId flowId, IMessageStore messageStore, ISerializer serializer)
    {
        _flowId = flowId;
        _messageStore = messageStore;
        _serializer = serializer;
    }

    private async Task<List<MessageAndIdempotencyKey>> GetReceivedMessages()
    {
        if (_receivedMessages is not null)
            return _receivedMessages;
        
        var storedMessages = await _messageStore.GetMessages(_flowId, skip: 0);
        return _receivedMessages = storedMessages
            .Select(m => 
                new MessageAndIdempotencyKey(
                    _serializer.DeserializeMessage(m.MessageJson, m.MessageType),
                    m.IdempotencyKey
                )
            ).ToList();
    }
    
    public async Task Clear()
    {
        var receivedMessages = await GetReceivedMessages();
        await _messageStore.Truncate(_flowId);
        receivedMessages.Clear();  
    }

    public async Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
    {
        var receivedMessages = await GetReceivedMessages(); 
        var (json, type) = _serializer.SerializeMessage(message);
        await _messageStore.AppendMessage(
            _flowId, new StoredMessage(json, type, idempotencyKey)
        );
        
        receivedMessages.Add(new MessageAndIdempotencyKey(message, idempotencyKey));  
    } 
    
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        var receivedMessages = await GetReceivedMessages();
        if (position >= receivedMessages.Count)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{receivedMessages.Count}'", nameof(position));
        
        var (json, type) = _serializer.SerializeMessage(message);
        await _messageStore.ReplaceMessage(_flowId, position, new StoredMessage(json, type, idempotencyKey));
        
        receivedMessages[position] = new MessageAndIdempotencyKey(message, idempotencyKey);  
    }
}