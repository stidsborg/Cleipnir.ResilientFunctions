using System;
using System.Runtime.CompilerServices;
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
        const string instance = "123";
        const string lockId = "321";
        
        var @lock = await monitor.Acquire(group: nameof(LockCanBeAcquiredAndReleasedSuccessfully),instance, lockId);
        @lock.ShouldNotBeNull();
        
        var lock2 = await monitor.Acquire(group: nameof(LockCanBeAcquiredAndReleasedSuccessfully),instance, lockId: "");
        lock2.ShouldBeNull();
        
        await @lock.DisposeAsync();
        
        lock2 = await monitor.Acquire(group: nameof(LockCanBeAcquiredAndReleasedSuccessfully),instance, lockId: "");
        lock2.ShouldNotBeNull();
    }

    public abstract Task TwoDifferentLocksCanBeAcquired();
    protected async Task TwoDifferentLocksCanBeAcquired(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string instance1 = "123";
        const string lockId1 = "321";
        const string instance2 = "1234";
        const string lockId2 = "3210";
        
        var lock1 = await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance1, lockId1);
        lock1.ShouldNotBeNull();
        var lock2 = await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance2, lockId2);
        lock2.ShouldNotBeNull();

        await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance1, lockId: "").ShouldBeNullAsync();
        await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance2, lockId: "").ShouldBeNullAsync();
        
        await lock1.DisposeAsync();
        await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance2, lockId: "").ShouldBeNullAsync();
        await lock2.DisposeAsync();
        
        await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance1, lockId: "").ShouldNotBeNullAsync();
        await monitor.Acquire(nameof(TwoDifferentLocksCanBeAcquired), instance2, lockId: "").ShouldNotBeNullAsync();
    }

    public abstract Task TakingATakenLockFails();
    protected async Task TakingATakenLockFails(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        const string instance = "123";
        const string lockId1 = "321";
        const string lockId2 = "3210";
        
        var @lock = await monitor.Acquire(nameof(TakingATakenLockFails), instance, lockId1);
        @lock.ShouldNotBeNull();
        
        await monitor.Acquire(nameof(TakingATakenLockFails), instance, lockId2).ShouldBeNullAsync();

        await @lock.DisposeAsync();
        await monitor.Acquire(nameof(TakingATakenLockFails), instance, lockId2).ShouldNotBeNullAsync();
    }

    public abstract Task ReTakingATakenLockWithSameKeyIdSucceeds();
    protected async Task ReTakingATakenLockWithSameKeyIdSucceeds(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        var group = Guid.NewGuid().ToString();
        const string lockName = "123";
        const string key = "321";
        
        var @lock = await monitor.Acquire(group, lockName, key);
        @lock.ShouldNotBeNull();
        
        var lock2 = await monitor.Acquire(group, lockName, key);
        lock2.ShouldNotBeNull();

        await @lock.DisposeAsync();
        
        var lock3 = await monitor.Acquire(group, lockName, key);
        lock3.ShouldNotBeNull();
    }

    public abstract Task AReleasedLockCanBeTakenAgain();
    protected async Task AReleasedLockCanBeTakenAgain(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        var group = Guid.NewGuid().ToString();
        const string lockName = "123";
        const string keyId = "321";
        
        var lock1 = await monitor.Acquire(group, lockName, keyId);
        lock1.ShouldNotBeNull();
        await lock1.DisposeAsync();
        
        var lock2 = await monitor.Acquire(group, lockName, keyId);
        lock2.ShouldNotBeNull();
    }

    public abstract Task WaitingAboveThresholdForATakenLockReturnsNull();
    protected async Task WaitingAboveThresholdForATakenLockReturnsNull(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        var group = Guid.NewGuid().ToString();
        const string lockName = "123";
        const string key1Id = "321";
        const string key2Id = "3210";
        
        await monitor.Acquire(group, lockName, key1Id);
        await monitor.Acquire(group, lockName, key2Id, 250).ShouldBeNullAsync();
    }

    public abstract Task WhenALockIsReleasedActiveAcquireShouldGetTheLock();
    protected async Task WhenALockIsReleasedActiveAcquireShouldGetTheLock(Task<IMonitor> monitorTask)
    {
        var monitor = await monitorTask;
        var group = Guid.NewGuid().ToString();
        const string lockName = "123";
        const string key1Id = "321";
        const string key2Id = "3210";
        
        var @lock = await monitor.Acquire(group, lockName, key1Id);
        @lock.ShouldNotBeNull();
        var acquireTask = monitor.Acquire(group, lockName, key2Id, TimeSpan.FromSeconds(5));
        await Task.Delay(150);
        await @lock.DisposeAsync();

        var lock2 = await acquireTask;
        lock2.ShouldNotBeNull();
    }
}