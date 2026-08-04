using System.Diagnostics.CodeAnalysis;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// The object-form counterpart of <see cref="Messaging.StoredMessage"/>: a message past the fetch-boundary
/// deserialization (<see cref="MessageDeserializer"/>), carrying its target flow and payload object. Flows flat
/// from the MessageWatchdog until <see cref="CoreRuntime.FlowsManager"/> groups per flow; the byte form exists
/// only at the storage boundaries (store rows and the <see cref="PendingMessages"/>-encoded effect carriers),
/// and the durable carriers serialize the payload at staging. A message that failed deserialization was dead
/// lettered at the boundary and is simply absent.
///
/// A null <see cref="Content"/> marks an empty restart-poke: it carries no payload and is never delivered -
/// both hand-over routes strip empties before the queue manager, so the delivery pipeline only ever sees
/// payload-carrying messages. A null <see cref="Position"/> marks a message without a backing message-store row
/// (e.g. appended via the control panel directly into the flow's effect state): it has no store identity, so the
/// QueueManager assigns it a synthetic negative position at staging and it never participates in row clearing or
/// push dedup.
/// </summary>
internal record IncomingMessage(
    StoredId StoredId,
    object? Content,
    long? Position,
    string? IdempotencyKey = null,
    string? Sender = null,
    string? Receiver = null)
{
    [MemberNotNullWhen(false, nameof(Content))]
    public bool IsEmpty => Content is null;

    // An empty restart-poke always addresses a store row - its position is all there is to it.
    public static IncomingMessage CreateEmpty(StoredId storedId, long position)
        => new(storedId, Content: null, position);
}
