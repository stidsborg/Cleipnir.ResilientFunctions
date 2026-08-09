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
/// Codec for the per-message effect carriers: a staged-message child effect (an admitted-but-undelivered
/// message captured under <see cref="QueueManager.StagedMessagesRoot"/>) carries its message encoded with this
/// codec - whether staged by the QueueManager, supplied as an initial message at flow creation or appended via
/// the control panel.
///
/// The encoding is BinaryPacker-based rather than serializer-based on purpose: the carriers are also decoded
/// outside the flow, by tooling that does not know the flow type's (possibly custom) serializer. The store
/// row's replica is deliberately not encoded: by the time a message reaches an effect carrier it has already
/// been fetched, and row-less messages never had a replica to begin with.
/// </summary>
internal static class PendingMessages
{
    // A message without a backing store row (e.g. appended via the control panel directly into the flow's effect
    // state) encodes a null position piece - it has no store identity to clear or dedup against. A staged message
    // always carries a payload, so the type id is never null here.
    public static byte[] EncodeMessage(
        byte[] messageContent,
        TypeId messageType,
        long? position,
        string? idempotencyKey = null,
        string? sender = null,
        string? receiver = null)
        => BinaryPacker.Pack(
            messageContent,
            messageType.Serialize(),
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
            MessageType: TypeId.Deserialize(parts[1]!),
            Position: parts[2] == null ? 0 : BitConverter.ToInt64(parts[2]!),
            Replica: ReplicaId.Empty,
            IdempotencyKey: parts[3]?.ToStringFromUtf8Bytes(),
            Sender: parts[4]?.ToStringFromUtf8Bytes(),
            Receiver: parts[5]?.ToStringFromUtf8Bytes()
        ) { RowBacked = parts[2] != null };
    }
}
