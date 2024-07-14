using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Queuing;

public interface IQueueStore
{
    Task<StoredQueueItem?> Peek(FlowId flowId);
    Task Pop(FlowId flowId);
    Task Push(FlowId flowId, StoredQueueItem storedQueueItem);
}