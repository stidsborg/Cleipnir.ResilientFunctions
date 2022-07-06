using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class AsyncSemaphoreTests
{
    [TestMethod]
    public async Task SemaphoreWithCapacityAllowsTakeImmediately()
    {
        var semaphore = new AsyncSemaphore(maxParallelism: 1);
        var flag = new SyncedFlag();

        {
            using var @lock = await semaphore.Take();
            flag.Raise();
        }
        
        flag.IsRaised.ShouldBeTrue();
    }
    
    [TestMethod]
    public async Task EnqueuedWorkIsOnlyExecutedAfterPreviousWorkHasCompleted()
    {
        var semaphore = new AsyncSemaphore(maxParallelism: 1);
        var m1Flag = new SyncedFlag();
        var m2Flag = new SyncedFlag();

        async Task M1()
        {
            using var @lock = await semaphore.Take();
            await Task.Delay(100);
            if (!m2Flag.IsRaised)
                m1Flag.Raise();
        }

        async Task M2()
        {
            using var @lock = await semaphore.Take();
            m2Flag.Raise();
        }

        _ = M1();
        _ = M2();

        await BusyWait.UntilAsync(() => m1Flag.IsRaised && m2Flag.IsRaised);
    }
    
    [TestMethod]
    public async Task WorkCanBeExecutedInParallel()
    {
        var semaphore = new AsyncSemaphore(maxParallelism: 2);
        var m1Flag = new SyncedFlag();
        var m2Flag = new SyncedFlag();

        async Task M1()
        {
            using var @lock = await semaphore.Take();
            await Task.Delay(100);
            if (m2Flag.IsRaised)
                m1Flag.Raise();
        }

        async Task M2()
        {
            using var @lock = await semaphore.Take();
            m2Flag.Raise();
        }

        _ = M1();
        _ = M2();

        await BusyWait.UntilAsync(() => m1Flag.IsRaised && m2Flag.IsRaised);
    }
}