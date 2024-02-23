using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Queuing;

public interface IQueueStore
{
    Task<StoredQueueItem?> Peek(FunctionId functionId);
    Task Pop(FunctionId functionId);
    Task Push(FunctionId functionId, StoredQueueItem storedQueueItem);
}