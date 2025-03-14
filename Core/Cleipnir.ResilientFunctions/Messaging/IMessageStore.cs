using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt);

    Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage);
    
    Task Truncate(StoredId storedId);
    
    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip);
    Task<IDictionary<StoredId, int>> GetMaxPositions(IReadOnlyList<StoredId> storedIds);
}