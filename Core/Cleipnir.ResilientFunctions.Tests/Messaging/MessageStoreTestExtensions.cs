using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public static class MessageStoreTestExtensions
{
    /// <summary>
    /// Test convenience for appending a single message - forwards to <see cref="IMessageStore.AppendMessages"/>.
    /// </summary>
    public static Task AppendMessage(this IMessageStore messageStore, StoredId storedId, StoredMessage storedMessage)
        => messageStore.AppendMessages([storedMessage.ToStoredIdAndSerializedMessage(storedId)]);

    /// <summary>
    /// Test convenience converting a <see cref="StoredMessage"/> into the append-side representation - the
    /// message's replica becomes the publisher replica.
    /// </summary>
    public static StoredIdAndSerializedMessage ToStoredIdAndSerializedMessage(this StoredMessage storedMessage, StoredId storedId)
        => new(
            storedId,
            new SerializedMessageWithReplicaId(
                new SerializedMessage(
                    storedMessage.MessageContent,
                    storedMessage.MessageType,
                    storedMessage.IdempotencyKey,
                    storedMessage.Sender,
                    storedMessage.Receiver
                ),
                storedMessage.Replica
            )
        );
}
