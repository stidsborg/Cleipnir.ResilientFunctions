using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// A message handed to <see cref="QueueManager.Push"/> for delivery - the slice of <see cref="StoredMessage"/> the
/// push pipeline needs, decoupled from the store row (no replica, no row-backed flag). A null <see cref="Position"/>
/// marks a message without a backing store row (e.g. control-panel appended straight into the flow's effect state);
/// such a message has no store identity and never participates in row clearing or push dedup.
/// </summary>
public record PushedMessage(byte[] MessageContent, byte[] MessageType, long? Position, string? IdempotencyKey, string? Sender, string? Receiver)
{
    public static PushedMessage From(StoredMessage message)
        => new(
            message.MessageContent,
            message.MessageType,
            message.RowBacked ? message.Position : null,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );
}
