using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class DistributedSemaphores(Effect effect, ISemaphoreStore semaphoreStore, StoredId storedId, Func<IReadOnlyList<StoredId>, Task> interrupt)
{
    public DistributedSemaphore Create(string group, string instance, int maximumCount) 
        => new(maximumCount, group, instance, effect, semaphoreStore, storedId, interrupt);
    
    public DistributedSemaphore CreateLock(string group, string instance) 
        => new(maximumCount: 1, group, instance, effect, semaphoreStore, storedId, interrupt);
}

public class DistributedSemaphore(int maximumCount, string group, string instance, Effect effect, ISemaphoreStore store, StoredId storedId, Func<IReadOnlyList<StoredId>, Task> interrupt)
{
    private string? _effectId; 
    public async Task<DistributedSemaphore.Lock> Acquire(TimeSpan? maxWait = null)
    {
        maxWait ??= TimeSpan.Zero;
        if (maxWait < TimeSpan.Zero) 
            throw new ArgumentOutOfRangeException(nameof(maxWait), maxWait, "MaxWait must be non negative");

        var implicitId = effect.TakeNextImplicitId();
        _effectId = $"Semaphore#{implicitId}";
        var statusOption = await effect.TryGet<SemaphoreIdAndStatus>(_effectId);
        var statusIdAndStatus = statusOption.HasValue ? statusOption.Value : new SemaphoreIdAndStatus(group, instance, SemaphoreStatus.Created);
        var status = statusIdAndStatus.Status;

        var gotLock = true;
        if (status is SemaphoreStatus.Created or SemaphoreStatus.Waiting)
            gotLock = await store.Acquire(group, instance, storedId, maximumCount);
        else if (status == SemaphoreStatus.Released)
            return new Lock(() => Task.CompletedTask);

        if (!gotLock && maxWait > TimeSpan.Zero)
        {
            var stopWatch = Stopwatch.StartNew();
            while (!gotLock && stopWatch.Elapsed < maxWait)
            {
                await Task.Delay(250);
                var lockQueue = await store.GetQueued(group, instance, maximumCount);
                gotLock = lockQueue.Any(id => id == storedId);
            }
        }

        if (!gotLock)
        {
            await effect.Upsert(_effectId, statusIdAndStatus with { Status = SemaphoreStatus.Waiting });
            throw new SuspendInvocationException();
        }

        await effect.Upsert(_effectId, statusIdAndStatus with { Status = SemaphoreStatus.Acquired });
        return new Lock(Release);
    }

    private async Task Release()
    {
        var lockQueue = await store.Release(group, instance, storedId, maximumCount);
        await interrupt(lockQueue);

        await effect.Upsert(_effectId!, new SemaphoreIdAndStatus(group, instance, SemaphoreStatus.Released));
    }
    
    public class Lock(Func<Task> releaseFunc) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => new(releaseFunc());
        public Task Release() => DisposeAsync().AsTask();
    }
}