using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// Codec for the reserved pending-messages effect: messages fetched for a flow that had already completed are
/// inlined into the flow's effect state (and deleted from the message store) so any later re-invocation - on any
/// replica and via any restart path - finds them in the effect snapshot the restart hands over. The QueueManager
/// stages them at initialization and prunes each message from the entry when it is delivered.
///
/// The encoding is BinaryPacker-based rather than serializer-based on purpose: the entry is written by the
/// FlowsManager, which does not know the flow type's (possibly custom) serializer.
/// </summary>
internal static class PendingMessages
{
    /// <summary>Reserved effect id (same -1 prefix as the QueueManager's other reserved ids).</summary>
    public static readonly EffectId EffectId = new([-1, 1]);

    public static byte[] Encode(IReadOnlyCollection<StoredMessage> messages)
        => BinaryPacker.Pack(messages.Select(EncodeMessage).ToArray());

    public static List<StoredMessage> Decode(byte[] bytes, StoredId storedId)
        => BinaryPacker
            .Split(bytes)
            .Select(messageBytes => DecodeMessage(messageBytes!, storedId))
            .ToList();

    // The store row's replica is deliberately not encoded: by the time a message reaches an effect carrier it
    // has already been fetched, and row-less messages never had a replica to begin with.
    public static byte[] EncodeMessage(StoredMessage message)
        => EncodeMessage(
            message.MessageContent,
            message.MessageType,
            message.RowBacked ? message.Position : null,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );

    // A message without a backing store row (e.g. appended via the control panel directly into the flow's effect
    // state) encodes a null position piece - it has no store identity to clear or dedup against.
    public static byte[] EncodeMessage(
        byte[] messageContent,
        byte[] messageType,
        long? position,
        string? idempotencyKey = null,
        string? sender = null,
        string? receiver = null)
        => BinaryPacker.Pack(
            messageContent,
            messageType,
            position is { } storePosition ? BitConverter.GetBytes(storePosition) : null,
            idempotencyKey?.ToUtf8Bytes(),
            sender?.ToUtf8Bytes(),
            receiver?.ToUtf8Bytes()
        );

    // The target flow is not encoded - an effect carrier lives inside its flow's own effect state, so the id is
    // supplied by the caller when decoding.
    public static StoredMessage DecodeMessage(byte[] bytes, StoredId storedId)
    {
        var parts = BinaryPacker.Split(bytes, expectedPieces: 6);
        return new StoredMessage(
            storedId,
            MessageContent: parts[0]!,
            MessageType: parts[1]!,
            Position: parts[2] == null ? 0 : BitConverter.ToInt64(parts[2]!),
            Replica: ReplicaId.Empty,
            IdempotencyKey: parts[3]?.ToStringFromUtf8Bytes(),
            Sender: parts[4]?.ToStringFromUtf8Bytes(),
            Receiver: parts[5]?.ToStringFromUtf8Bytes()
        ) { RowBacked = parts[2] != null };
    }
}
