using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public class Synchronization(DistributedSemaphores semaphores)
{
    public Task<DistributedSemaphore.Lock> AcquireLock(string group, string instance, TimeSpan? maxWait = null)
        => AcquireSemaphore(group, instance, maximumCount: 1, maxWait);

    public Task<DistributedSemaphore.Lock> AcquireSemaphore(string group, string instance, int maximumCount, TimeSpan? maxWait = null)
    {
        var distributedSemaphore = semaphores.Create(group, instance, maximumCount);
        return distributedSemaphore.Acquire(maxWait);
    }
}