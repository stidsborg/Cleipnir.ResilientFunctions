using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage);

    Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage);
    
    Task Truncate(StoredId storedId);
    
    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip);
}