using System;
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

    public Task Release(string group, string name, string lockId);

    public interface ILock : IAsyncDisposable { }
}