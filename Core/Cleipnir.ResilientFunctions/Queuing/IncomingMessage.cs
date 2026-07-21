using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// A message arriving at the queue manager's delivery pipeline - the payload slice of <see cref="StoredMessage"/>
/// the pipeline and the durable message carriers use. Incoming is pre-admission: the QueueManager's gate
/// (fetched-position dedup and idempotency-key claim) decides whether the message becomes a
/// <see cref="QueueManager.StagedMessage"/> waiting for a subscription or is dropped. A message re-staged from its
/// own child effect (<see cref="ChildId"/>) has already passed the gate and skips it.
///
/// Deliberately position-free (and replica-free): a position is the address of a message-store row, not part of
/// the message, so it travels alongside the payload at runtime and is persisted separately - the
/// <see cref="QueueManager.StagedPositionsId"/> entry links each row-backed staged child to its row, and the
/// pending-messages blob files each inlined message under the position of the row it came from. A message with no
/// backing row (e.g. appended via the control panel directly into the flow's effect state) simply has no position
/// anywhere and never participates in row clearing or push dedup.
/// </summary>
internal record IncomingMessage(
    byte[] MessageContent,
    byte[] MessageType,
    string? IdempotencyKey = null,
    string? Sender = null,
    string? Receiver = null)
{
    /// <summary>
    /// The staged-message child this message was re-staged from, when it has one. Runtime-only - never part of
    /// the encoded payload. Row-less messages are identified by it in place of a store position.
    /// </summary>
    public EffectId? ChildId { get; init; }

    public static IncomingMessage From(StoredMessage message)
        => new(
            message.MessageContent,
            message.MessageType,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );

    public StoredMessage ToStoredMessage(long? position)
        => new(
            MessageContent,
            MessageType,
            position ?? 0,
            ReplicaId.Empty,
            IdempotencyKey,
            Sender,
            Receiver
        ) { RowBacked = position is not null };
}
