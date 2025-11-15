using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task AppendMessage(StoredId storedId, StoredMessage storedMessage);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages);

    Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage);
    Task DeleteMessages(StoredId storedId, IEnumerable<long> positions);

    Task Truncate(StoredId storedId);

    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, long skip);
    Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds);
    Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds);
}