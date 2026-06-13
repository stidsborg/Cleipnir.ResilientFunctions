using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
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
    private readonly ReplicaId _publisherReplica;
    private readonly IFlowsManager _flowsManager;

    public MessageWriters(
        StoredType storedType,
        IFunctionStore functionStore,
        ISerializer serializer,
        ReplicaId publisherReplica,
        IFlowsManager flowsManager)
    {
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
        _publisherReplica = publisherReplica;
        _flowsManager = flowsManager;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = StoredId.Create(_storedType, instance.Value);
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer, _publisherReplica, _flowsManager);
    }

    internal MessageWriter For(StoredId storedId)
    {
        return new MessageWriter(storedId, _functionStore.MessageStore, _serializer, _publisherReplica, _flowsManager);
    }

    public async Task AppendMessages(IReadOnlyList<BatchedMessage> messages)
    {
        var storedIdAndMessages = new List<StoredIdAndMessage>(messages.Count);
        foreach (var (instance, message, idempotencyKey) in messages)
        {
            var storedId = StoredId.Create(_storedType, instance.Value);
            var content = _serializer.Serialize(message, message.GetType());
            var type = _serializer.SerializeType(message.GetType());
            var storedMessage = new StoredMessage(content, type, Position: 0, Replica: _publisherReplica, IdempotencyKey: idempotencyKey);
            storedIdAndMessages.Add(new StoredIdAndMessage(storedId,storedMessage));
        }

        await _functionStore.MessageStore.AppendMessages(storedIdAndMessages);
    }
}