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

    public static List<StoredMessage> Decode(byte[] bytes)
        => BinaryPacker
            .Split(bytes)
            .Select(messageBytes => DecodeMessage(messageBytes!))
            .ToList();

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
