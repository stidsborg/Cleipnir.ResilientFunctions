using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly StoredType _storedType;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ReplicaId _publisherReplica;
    private readonly MessageWatchdog? _messageWatchdog;

    internal MessageWriters(
        StoredType storedType,
        IFunctionStore functionStore,
        ISerializer serializer,
        ReplicaId publisherReplica,
        MessageWatchdog? messageWatchdog = null)
    {
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
        _publisherReplica = publisherReplica;
        _messageWatchdog = messageWatchdog;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = StoredId.Create(_storedType, instance.Value);
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer, _publisherReplica, _messageWatchdog);
    }

    internal MessageWriter For(StoredId storedId)
    {
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer, _publisherReplica, _messageWatchdog);
    }

    public async Task AppendMessages(IReadOnlyList<BatchedMessage> messages)
    {
        var serializedMessages = new List<SerializedMessageWithReplicaId>(messages.Count);
        foreach (var (instance, message, idempotencyKey) in messages)
        {
            var storedId = StoredId.Create(_storedType, instance.Value);
            var content = _serializer.Serialize(message, message.GetType());
            var type = _serializer.SerializeType(message.GetType());
            var serializedMessage = new SerializedMessage(storedId, content, type, IdempotencyKey: idempotencyKey, Sender: null, Receiver: null);
            serializedMessages.Add(new SerializedMessageWithReplicaId(serializedMessage, _publisherReplica));
        }

        await _functionStore.MessageStore.AppendMessages(serializedMessages);

        // Wake the MessageWatchdog so the appended messages are delivered now rather than on the next poll.
        _messageWatchdog?.Notify();
    }
}