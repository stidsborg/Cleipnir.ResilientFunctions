using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task<FunctionStatus?> AppendMessage(FunctionId functionId, StoredMessage storedMessage);

    Task<bool> ReplaceMessage(FunctionId functionId, int position, StoredMessage storedMessage);
    
    Task Truncate(FunctionId functionId);
    
    Task<IReadOnlyList<StoredMessage>> GetMessages(FunctionId functionId, int skip);
    Task<bool> HasMoreMessages(FunctionId functionId, int skip);
}