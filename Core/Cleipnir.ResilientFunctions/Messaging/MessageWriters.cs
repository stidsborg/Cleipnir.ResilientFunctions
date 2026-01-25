using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly StoredType _storedType;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;

    public MessageWriters(
        StoredType storedType,
        IFunctionStore functionStore, 
        ISerializer serializer)
    {
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = StoredId.Create(_storedType, instance.Value);
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer);
    }
    
    internal MessageWriter For(StoredId storedId)
    {
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer);
    }

    public async Task AppendMessages(IReadOnlyList<BatchedMessage> messages)
    {
        var storedIdAndMessages = new List<StoredIdAndMessage>(messages.Count);
        foreach (var (instance, message, idempotencyKey) in messages)
        {
            var storedId = StoredId.Create(_storedType, instance.Value);
            _serializer.Serialize(message, out var content, out var type);
            var storedMessage = new StoredMessage(content, type, Position: 0, idempotencyKey);
            storedIdAndMessages.Add(new StoredIdAndMessage(storedId,storedMessage));
        }

        await _functionStore.MessageStore.AppendMessages(storedIdAndMessages);
    } 
}