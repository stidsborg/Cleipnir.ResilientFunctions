using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Registry-level message publisher used by all message-producing components. Single-message appends are
/// stamped with this node's own replica id, so the publishing replica delivers them immediately (woken by
/// notify); serialized batches are stamped with each target flow's responsible replica
/// (<see cref="ClusterInfo.ResponsibleReplica"/>), sharding delivery work across the cluster. An explicit
/// replica can also be provided per append.
/// </summary>
internal class MessagesSender(
    IFunctionStore functionStore,
    ISerializer serializer,
    ClusterInfo clusterInfo,
    MessageWatchdog? messageWatchdog
)
{
    public SerializedMessage Serialize(StoredId storedId, object message, string? idempotencyKey = null, string? sender = null, string? receiver = null)
    {
        var content = serializer.Serialize(message, message.GetType());
        var type = serializer.SerializeType(message.GetType());
        return new SerializedMessage(storedId, content, type, idempotencyKey, sender, receiver);
    }

    /// <summary>
    /// Appends the message stamped with this node's own replica id - the publishing replica delivers it itself,
    /// woken immediately by the notify below.
    /// </summary>
    public async Task AppendMessage(StoredId storedId, object message, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => await AppendMessage(storedId, message, clusterInfo.ReplicaId, idempotencyKey, sender, receiver);

    /// <summary>
    /// Appends the message stamped with the provided replica instead of the target flow's responsible replica.
    /// </summary>
    public async Task AppendMessage(StoredId storedId, object message, ReplicaId replicaId, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => await AppendMessages([new SerializedMessageWithReplicaId(Serialize(storedId, message, idempotencyKey, sender, receiver), replicaId)]);

    public async Task AppendMessages(IReadOnlyList<SerializedMessage> messages)
        => await AppendMessages(
            messages
                .Select(m => new SerializedMessageWithReplicaId(m, clusterInfo.ResponsibleReplica(m.StoredId)))
                .ToList()
        );

    public async Task AppendMessages(IReadOnlyList<SerializedMessageWithReplicaId> messages)
    {
        if (messages.Count == 0)
            return;

        await functionStore.MessageStore.AppendMessages(messages);

        // Wake this replica's MessageWatchdog so messages it is responsible for (or whose target flows it
        // owns) are delivered now rather than on the next poll - other replicas' messages await their polls.
        messageWatchdog?.Notify();
    }
}
