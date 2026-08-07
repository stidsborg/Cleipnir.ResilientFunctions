using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Registry-level message publisher used by all message-producing components. Single-message appends are
/// stamped with this node's own replica id, so the publishing replica delivers them immediately (woken by
/// notify); serialized batches are stamped with each target flow's responsible replica
/// (<see cref="ClusterInfo.ResponsibleReplica"/>), sharding delivery work across the cluster. An explicit
/// replica can also be provided per append.
/// </summary>
internal class MessageSender(
    IFunctionStore functionStore,
    ISerializer serializer,
    ClusterInfo clusterInfo
)
{
    public MessageWatchdog? MessageWatchdog { get; set; }
    
    public SerializedMessage Serialize(StoredId storedId, object message, string? idempotencyKey = null, string? sender = null, string? receiver = null)
    {
        var content = serializer.Serialize(message, message.GetType());
        var type = message.GetType().SerializeType();
        return new SerializedMessage(storedId, content, type, idempotencyKey, sender, receiver);
    }

    /// <summary>
    /// Appends the message stamped with this node's own replica id - the publishing replica delivers it itself,
    /// woken immediately by the notify below.
    /// </summary>
    public async Task SendMessage(StoredId storedId, object message, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => await SendMessage(storedId, message, clusterInfo.ReplicaId, idempotencyKey, sender, receiver);

    /// <summary>
    /// Appends the message stamped with the provided replica instead of the target flow's responsible replica.
    /// </summary>
    public async Task SendMessage(StoredId storedId, object message, ReplicaId replicaId, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => await SendMessages([new SerializedMessageWithReplicaId(Serialize(storedId, message, idempotencyKey, sender, receiver), replicaId)]);

    public async Task SendMessages(StoredType storedType, IReadOnlyList<BatchedMessage> messages)
        => await SendMessages(
            messages
                .Select(m => Serialize(StoredId.Create(storedType, m.Instance.Value), m.Message, m.IdempotencyKey))
                .ToList()
        );

    public async Task SendMessages(IReadOnlyList<SerializedMessage> messages)
        => await SendMessages(
            messages
                .Select(m => new SerializedMessageWithReplicaId(m, clusterInfo.ResponsibleReplica(m.StoredId)))
                .ToList()
        );

    /// <summary>
    /// Appends an empty restart-poke per flow, stamped with this node's own replica id: fetched by this
    /// replica's own MessageWatchdog (woken by the notify below), the poke forces the flow's restart - with any
    /// pending messages of the flow arriving in the same fetch batch, handed to the restart in-hand. The poke
    /// carries no payload and is never delivered; the restart that consumes it deletes its row.
    /// </summary>
    public async Task SendRestartPokes(IReadOnlyList<StoredId> storedIds)
        => await SendMessages(
            storedIds
                .Select(storedId => new SerializedMessageWithReplicaId(
                    new SerializedMessage(storedId, Content: [], Type: [], IdempotencyKey: null, Sender: null, Receiver: null),
                    clusterInfo.ReplicaId
                ))
                .ToList()
        );

    public async Task SendMessages(IReadOnlyList<SerializedMessageWithReplicaId> messages)
    {
        if (messages.Count == 0)
            return;

        await functionStore.MessageStore.AppendMessages(messages);

        // Wake this replica's MessageWatchdog so messages it is responsible for (or whose target flows it
        // owns) are delivered now rather than on the next poll - other replicas' messages await their polls.
        MessageWatchdog?.Notify();
    }
}
