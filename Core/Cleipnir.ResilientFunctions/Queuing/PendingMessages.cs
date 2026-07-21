using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// Codec for the reserved pending-messages effect: messages fetched for a flow that had already completed are
/// inlined into the flow's effect state (and deleted from the message store) so any later re-invocation - on any
/// replica and via any restart path - finds them in the effect snapshot the restart hands over. The QueueManager
/// stages them at initialization and prunes each message from the entry when it is delivered.
///
/// Each message is filed under the position of the (since deleted) store row it was inlined from - the payloads
/// themselves are position-free, and the position-key is the identity the inliner's idempotent merge, the
/// delivery prune and control-panel removal address the message by.
///
/// The encoding is BinaryPacker-based rather than serializer-based on purpose: the entry is written by the
/// FlowsManager, which does not know the flow type's (possibly custom) serializer.
/// </summary>
internal static class PendingMessages
{
    /// <summary>Reserved effect id (same -1 prefix as the QueueManager's other reserved ids).</summary>
    public static readonly EffectId EffectId = new([-1, 1]);

    public static byte[] Encode(IReadOnlyDictionary<long, IncomingMessage> messages)
        => BinaryPacker.Pack(
            messages
                .OrderBy(kv => kv.Key)
                .Select(kv => BinaryPacker.Pack(BitConverter.GetBytes(kv.Key), EncodeMessage(kv.Value)))
                .ToArray()
        );

    public static Dictionary<long, IncomingMessage> Decode(byte[] bytes)
    {
        var messages = new Dictionary<long, IncomingMessage>();
        foreach (var entryBytes in BinaryPacker.Split(bytes))
        {
            var parts = BinaryPacker.Split(entryBytes!, expectedPieces: 2);
            messages[BitConverter.ToInt64(parts[0]!)] = DecodeMessage(parts[1]!);
        }

        return messages;
    }

    public static byte[] EncodeMessage(IncomingMessage message)
        => BinaryPacker.Pack(
            message.MessageContent,
            message.MessageType,
            message.IdempotencyKey?.ToUtf8Bytes(),
            message.Sender?.ToUtf8Bytes(),
            message.Receiver?.ToUtf8Bytes()
        );

    public static IncomingMessage DecodeMessage(byte[] bytes)
    {
        var parts = BinaryPacker.Split(bytes, expectedPieces: 5);
        return new IncomingMessage(
            MessageContent: parts[0]!,
            MessageType: parts[1]!,
            IdempotencyKey: parts[2]?.ToStringFromUtf8Bytes(),
            Sender: parts[3]?.ToStringFromUtf8Bytes(),
            Receiver: parts[4]?.ToStringFromUtf8Bytes()
        );
    }
}
