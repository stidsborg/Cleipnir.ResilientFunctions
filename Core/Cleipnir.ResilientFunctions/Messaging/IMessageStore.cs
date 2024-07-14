using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task<FunctionStatus?> AppendMessage(FlowId flowId, StoredMessage storedMessage);

    Task<bool> ReplaceMessage(FlowId flowId, int position, StoredMessage storedMessage);
    
    Task Truncate(FlowId flowId);
    
    Task<IReadOnlyList<StoredMessage>> GetMessages(FlowId flowId, int skip);
    Task<bool> HasMoreMessages(FlowId flowId, int skip);
}