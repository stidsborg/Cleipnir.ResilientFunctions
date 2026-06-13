using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter(StoredId storedIdId, IMessageStore messageStore, ISerializer eventSerializer, ReplicaId publisherReplica, IFlowsManager flowsManager)
{
    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null, string? sender = null, string? receiver = null) where TMessage : class
    {
        var eventJson = eventSerializer.Serialize(message, message.GetType());
        var eventType = eventSerializer.SerializeType(message.GetType());

        var writtenReplica = await messageStore.AppendMessage(
            storedIdId,
            new StoredMessage(eventJson, eventType, Position: 0, Replica: publisherReplica, IdempotencyKey: idempotencyKey, Sender: sender, Receiver: receiver)
        );

        // The message fell back to (or is owned by) this replica - schedule the target so it runs and consumes
        // the message. Targets owned by another replica are delivered by that replica's MessageWatchdog.
        if (writtenReplica == publisherReplica)
            await flowsManager.Schedule(storedIdId);
    }
}