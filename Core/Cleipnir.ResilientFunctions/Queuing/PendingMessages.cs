using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// The reserved received-messages carrier: every not-yet-delivered message is held as a child effect of
/// <see cref="Root"/>, keyed by its position (<see cref="ChildId"/>). A running incarnation writes these children
/// as it stages messages; the FlowsManager writes the same children when it inlines messages fetched for an
/// already-completed flow (and deletes the message-store rows) so any later re-invocation - on any replica and via
/// any restart path - stages them from the effect snapshot the restart hands over. The QueueManager stages the
/// children at initialization and deletes each one when its message is delivered.
///
/// Each child's content is the single-message <see cref="EncodeMessage"/> encoding: BinaryPacker-based rather than
/// serializer-based on purpose, since the FlowsManager writes it without knowing the flow type's (possibly custom)
/// serializer, and a running incarnation must read back byte-identical content.
/// </summary>
internal static class PendingMessages
{
    /// <summary>Reserved parent effect id (same -1 prefix as the QueueManager's other reserved ids).</summary>
    public static readonly EffectId Root = new([-1, 2]);

    /// <summary>The child effect id a message at <paramref name="position"/> is stored under.</summary>
    public static EffectId ChildId(long position) => Root.CreateChild((int)position);

    public static byte[] EncodeMessage(StoredMessage message)
        => BinaryPacker.Pack(
            message.MessageContent,
            message.MessageType,
            BitConverter.GetBytes(message.Position),
            message.IdempotencyKey?.ToUtf8Bytes(),
            message.Sender?.ToUtf8Bytes(),
            message.Receiver?.ToUtf8Bytes()
        );

    public static StoredMessage DecodeMessage(byte[] bytes)
    {
        var parts = BinaryPacker.Split(bytes, expectedPieces: 6);
        return new StoredMessage(
            MessageContent: parts[0]!,
            MessageType: parts[1]!,
            Position: BitConverter.ToInt64(parts[2]!),
            Replica: ReplicaId.Empty,
            IdempotencyKey: parts[3]?.ToStringFromUtf8Bytes(),
            Sender: parts[4]?.ToStringFromUtf8Bytes(),
            Receiver: parts[5]?.ToStringFromUtf8Bytes()
        );
    }
}
