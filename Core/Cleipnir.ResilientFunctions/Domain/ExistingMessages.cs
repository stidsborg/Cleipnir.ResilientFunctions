using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingMessages : IEnumerable<object>
{
    private readonly FunctionId _functionId;
    private readonly List<MessageAndIdempotencyKey> _receivedMessages;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    public List<MessageAndIdempotencyKey> MessagesWithIdempotencyKeys => _receivedMessages;

    private int ExistingCount { get; }
    
    public ExistingMessages(FunctionId functionId, List<MessageAndIdempotencyKey> receivedMessages, IMessageStore messageStore, ISerializer serializer)
    {
        _functionId = functionId;
        _receivedMessages = receivedMessages;
        _messageStore = messageStore;
        _serializer = serializer;
        
        ExistingCount = _receivedMessages.Count;
    }

    public object this[int index]
    {
        get => _receivedMessages[index].Message;
        set => _receivedMessages[index] = new MessageAndIdempotencyKey(Message: value, IdempotencyKey: null);
    }

    public void Clear() => _receivedMessages.Clear();
    public void Add(object message) => _receivedMessages.Add(new MessageAndIdempotencyKey(message, IdempotencyKey: null));
    public void AddRange(IEnumerable<object> messages) 
        => _receivedMessages.AddRange(messages.Select(e => new MessageAndIdempotencyKey(e)));

    public void Replace(IEnumerable<object> messages)
    {
        _receivedMessages.Clear();
        AddRange(messages);
    }
    
    public async Task SaveChanges(bool verifyNoChangesBeforeSave = false)
    {
        var storedMessages = _receivedMessages.Select(messageAndIdempotencyKey =>
        {
            var (json, type) = _serializer.SerializeMessage(messageAndIdempotencyKey.Message);
            return new StoredMessage(json, type, messageAndIdempotencyKey.IdempotencyKey);
        });

        var success = await _messageStore.Replace(
            _functionId,
            storedMessages,
            verifyNoChangesBeforeSave ? ExistingCount : default(int?)
        );

        if (!success)
            throw new ConcurrentModificationException(_functionId);
    }

    public IEnumerator<object> GetEnumerator() => _receivedMessages.Select(e => e.Message).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}