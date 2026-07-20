using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// A message on its way to a flow's subscriptions - the slice of <see cref="StoredMessage"/> the delivery pipeline
/// actually uses. The store row's replica is deliberately absent: by the time a message reaches the QueueManager it
/// has already been fetched, and messages living purely in effect state never had a replica to begin with.
///
/// A null <see cref="Position"/> marks a message without a backing message-store row (e.g. appended via the control
/// panel directly into the flow's effect state). Such a message has no store identity, so the QueueManager assigns
/// it a synthetic negative position at staging and it never participates in row clearing or push dedup.
/// </summary>
internal record ReceivedMessage(
    byte[] MessageContent,
    byte[] MessageType,
    long? Position,
    string? IdempotencyKey = null,
    string? Sender = null,
    string? Receiver = null)
{
    /// <summary>
    /// The received-message child this message was re-staged from, when it has one. Runtime-only - never part of
    /// the encoded payload. Row-less messages are identified by it in place of a store position.
    /// </summary>
    public EffectId? ChildId { get; init; }

    public static ReceivedMessage From(StoredMessage message)
        => new(
            message.MessageContent,
            message.MessageType,
            message.RowBacked ? message.Position : null,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );

    public StoredMessage ToStoredMessage()
        => new(
            MessageContent,
            MessageType,
            Position ?? 0,
            ReplicaId.Empty,
            IdempotencyKey,
            Sender,
            Receiver
        ) { RowBacked = Position is not null };
}
