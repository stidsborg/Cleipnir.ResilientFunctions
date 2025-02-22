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
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriters(
        StoredType storedType,
        IFunctionStore functionStore, 
        ISerializer serializer, 
        ScheduleReInvocation scheduleReInvocation)
    {
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = new StoredId(_storedType, instance.Value.ToStoredInstance());
        return new MessageWriter(storedId, _functionStore, _serializer, _scheduleReInvocation);
    }
    
    internal MessageWriter For(StoredInstance instance)
    {
        var storedId = new StoredId(_storedType, instance);
        return new MessageWriter(storedId, _functionStore, _serializer, _scheduleReInvocation);
    }

    public async Task AppendMessages(IReadOnlyList<BatchedMessage> messages, bool interrupt = true)
    {
        var storedIdAndMessages = new List<StoredIdAndMessage>(messages.Count);
        foreach (var (instance, message, idempotencyKey) in messages)
        {
            var storedId = new StoredId(_storedType, instance.Value.ToStoredInstance());
            var (content, type) = _serializer.SerializeMessage(message, message.GetType());
            var storedMessage = new StoredMessage(content, type, idempotencyKey);
            storedIdAndMessages.Add(new StoredIdAndMessage(storedId,storedMessage));
        }

        await _functionStore.MessageStore.AppendMessages(storedIdAndMessages, interrupt);
    } 
}