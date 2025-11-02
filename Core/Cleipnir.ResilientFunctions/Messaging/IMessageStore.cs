using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task AppendMessage(StoredId storedId, StoredMessage storedMessage);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt);

    Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage);

    Task Truncate(StoredId storedId);

    Task<IReadOnlyList<StoredMessageWithPosition>> GetMessages(StoredId storedId, long skip);
    Task<Dictionary<StoredId, List<StoredMessageWithPosition>>> GetMessages(IEnumerable<StoredId> storedIds);
    Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds);
}