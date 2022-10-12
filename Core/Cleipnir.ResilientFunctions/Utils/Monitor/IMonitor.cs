using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public interface IMonitor
{
    public Task<ILock?> Acquire(string group, string key);
    
    public async Task<ILock?> Acquire(string group, string key, TimeSpan maxWait)
    {
        var prev = DateTime.UtcNow;
        while (true)
        {
            var @lock = await Acquire(group, key);
            if (@lock != null) return @lock;
            await Task.Delay(100);
            if (DateTime.UtcNow - prev > maxWait) return null;
        }
    }

    public Task<ILock?> Acquire(string group, string key, int maxWaitMs)
        => Acquire(group, key, TimeSpan.FromMilliseconds(maxWaitMs));

    public Task Release(string group, string key);

    public interface ILock : IAsyncDisposable { }
}