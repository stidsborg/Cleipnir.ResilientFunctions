using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public interface IMonitor
{
    public Task<ILock?> Acquire(string group, string name, string lockId);
    
    public async Task<ILock?> Acquire(string group, string name, string lockId, TimeSpan maxWait)
    {
        var prev = DateTime.UtcNow;
        while (true)
        {
            var @lock = await Acquire(group, name, lockId);
            if (@lock != null) return @lock;
            await Task.Delay(100);
            if (DateTime.UtcNow - prev > maxWait) return null;
        }
    }

    public Task<ILock?> Acquire(string group, string name, string lockId, int maxWaitMs)
        => Acquire(group, name, lockId, TimeSpan.FromMilliseconds(maxWaitMs));
    
    public async Task<AcquiredLocks?> Acquire(params LockInfo[] locks)
    {
        var orderedLocks = locks.OrderBy(r => r.GroupId).ThenBy(r => r.Name);
        var acquiredLocks = new List<ILock>(locks.Length);
        foreach (var (groupId, name, lockId, timeSpan) in orderedLocks)
        {
            var acquiredLock = timeSpan == null 
                ? await Acquire(groupId, name, lockId)
                : await Acquire(groupId, name, lockId, timeSpan.Value);

            if (acquiredLock == null)
            {
                foreach (var prevAcquiredLock in acquiredLocks)
                    await prevAcquiredLock.DisposeAsync();
                
                return null;
            }
            
            acquiredLocks.Add(acquiredLock);
        }

        return new AcquiredLocks(acquiredLocks);
    }

    public Task Release(string group, string name, string lockId);

    public interface ILock : IAsyncDisposable { }
}