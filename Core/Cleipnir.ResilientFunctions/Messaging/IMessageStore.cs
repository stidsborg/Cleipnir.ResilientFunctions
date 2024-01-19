using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage);
    Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null);
    Task<FunctionStatus> AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages);
    
    Task Truncate(FunctionId functionId);
    Task<bool> Replace(FunctionId functionId, IEnumerable<StoredMessage> storedMessages, int? expectedMessageCount);

    Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId);
    MessagesSubscription SubscribeToMessages(FunctionId functionId);
}