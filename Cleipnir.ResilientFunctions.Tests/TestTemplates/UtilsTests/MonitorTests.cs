using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests;

public abstract class MonitorTests
{
    public abstract Task LockCanBeAcquiredAndReleasedSuccessfully();    
    protected async Task LockCanBeAcquiredAndReleasedSuccessfully(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string keyId = "321";
        
        var @lock = await monitor.Acquire(lockId, keyId);
        @lock.ShouldNotBeNull();
        await LockCount().ShouldBeAsync(1);
        
        await @lock.DisposeAsync();

        await LockCount().ShouldBeAsync(0);
    }

    public abstract Task TwoDifferentLocksCanBeAcquired();
    protected async Task TwoDifferentLocksCanBeAcquired(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lock1Id = "123";
        const string key1Id = "321";
        const string lock2Id = "1234";
        const string key2Id = "3210";
        
        var lock1 = await monitor.Acquire(lock1Id, key1Id);
        lock1.ShouldNotBeNull();
        var lock2 = await monitor.Acquire(lock2Id, key2Id);
        lock2.ShouldNotBeNull();
        
        await LockCount().ShouldBeAsync(2);
        await lock1.DisposeAsync();
        await LockCount().ShouldBeAsync(1);
        await lock2.DisposeAsync();
        await LockCount().ShouldBeAsync(0);
    }

    public abstract Task TakingATakenLockFails();
    protected async Task TakingATakenLockFails(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string key1Id = "321";
        const string key2Id = "3210";
        
        var @lock = await monitor.Acquire(lockId, key1Id);
        @lock.ShouldNotBeNull();
        
        await monitor.Acquire(lockId, key2Id).ShouldBeNullAsync();

        await @lock.DisposeAsync();
        await LockCount().ShouldBeAsync(0);
    }

    public abstract Task ReTakingATakenLockWithSameKeyIdSucceeds();
    protected async Task ReTakingATakenLockWithSameKeyIdSucceeds(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string keyId = "321";
        
        var @lock = await monitor.Acquire(lockId, keyId);
        @lock.ShouldNotBeNull();
        
        var lock2 = await monitor.Acquire(lockId, keyId);
        lock2.ShouldNotBeNull();

        await @lock.DisposeAsync();
        await LockCount().ShouldBeAsync(0);
    }

    public abstract Task AReleasedLockCanBeTakenAgain();
    protected async Task AReleasedLockCanBeTakenAgain(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string keyId = "321";
        
        var lock1 = await monitor.Acquire(lockId, keyId);
        lock1.ShouldNotBeNull();
        await lock1.DisposeAsync();
        
        var lock2 = await monitor.Acquire(lockId, keyId);
        lock2.ShouldNotBeNull();
        
        await LockCount().ShouldBeAsync(1);
    }

    public abstract Task WaitingAboveThresholdForATakenLockReturnsNull();
    protected async Task WaitingAboveThresholdForATakenLockReturnsNull(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string key1Id = "321";
        const string key2Id = "3210";
        
        await monitor.Acquire(lockId, key1Id);
        await monitor.Acquire(lockId, key2Id, 250).ShouldBeNullAsync();
    }

    public abstract Task WhenALockIsReleasedActiveAcquireShouldGetTheLock();
    protected async Task WhenALockIsReleasedActiveAcquireShouldGetTheLock(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string lockId = "123";
        const string key1Id = "321";
        const string key2Id = "3210";
        
        var @lock = await monitor.Acquire(lockId, key1Id);
        @lock.ShouldNotBeNull();
        var acquireTask = monitor.Acquire(lockId, key2Id, TimeSpan.FromSeconds(5));
        await Task.Delay(150);
        await @lock.DisposeAsync();

        var lock2 = await acquireTask;
        lock2.ShouldNotBeNull();
    }

    protected abstract Task<int> LockCount([System.Runtime.CompilerServices.CallerMemberName] string memberName = "");
}