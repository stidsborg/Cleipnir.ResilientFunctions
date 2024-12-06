using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ICemaphoreStore
{
    Task<bool> Acquire(string group, string instance, StoredId storedId, int semaphoreCount);
    Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int semaphoreCount);
    Task<IReadOnlyList<StoredId>> GetQueued(string group, string instance, int count);
}