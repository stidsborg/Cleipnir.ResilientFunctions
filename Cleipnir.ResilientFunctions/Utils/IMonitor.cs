using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils;

public interface IMonitor
{
    public Task<ILock?> Acquire(string lockId, string keyId);
    
    public async Task<ILock?> Acquire(string lockId, string keyId, TimeSpan maxWait)
    {
        var prev = DateTime.UtcNow;
        while (true)
        {
            var @lock = await Acquire(lockId, keyId);
            if (@lock != null) return @lock;
            await Task.Delay(100);
            if (DateTime.UtcNow - prev > maxWait) return null;
        }
    }

    public Task<ILock?> Acquire(string lockId, string keyId, int maxWaitMs)
        => Acquire(lockId, keyId, TimeSpan.FromMilliseconds(maxWaitMs));    public Task Release(string lockId, string keyId);
    
    public interface ILock : IAsyncDisposable { }
}