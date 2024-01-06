using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public class AcquiredLocks : IAsyncDisposable
{
    private readonly IEnumerable<IMonitor.ILock> _locks;

    internal AcquiredLocks(IEnumerable<IMonitor.ILock> locks) => _locks = locks;

    public async ValueTask DisposeAsync()
    {
        foreach (var prevAcquiredLock in _locks)
            await prevAcquiredLock.DisposeAsync();
    }
}