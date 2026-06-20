using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public static class MessageStoreTestExtensions
{
    /// <summary>
    /// Test convenience for appending a single message - forwards to <see cref="IMessageStore.AppendMessages"/>.
    /// </summary>
    public static Task AppendMessage(this IMessageStore messageStore, StoredId storedId, StoredMessage storedMessage)
        => messageStore.AppendMessages([new StoredIdAndMessage(storedId, storedMessage)]);
}
