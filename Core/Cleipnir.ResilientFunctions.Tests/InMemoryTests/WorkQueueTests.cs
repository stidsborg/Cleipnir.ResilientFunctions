using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class WorkQueueTests
{
    [TestMethod]
    public async Task EnqueuedWorkIsExecuted()
    {
        var workQueue = new WorkQueue(maxParallelism: 1);
        var flag = new SyncedFlag();
        workQueue.Enqueue(() =>
        {
            flag.Raise();
            return Task.CompletedTask;
        });

        await BusyWait.UntilAsync(() => flag.Position == FlagPosition.Raised);
    }
    
    [TestMethod]
    public async Task EnqueuedWorkIsOnlyExecutedAfterPreviousWorkHasCompleted()
    {
        var workQueue = new WorkQueue(maxParallelism: 2);
        var waitFlag = new SyncedFlag();
        var executedFlag = new SyncedFlag();
        workQueue.Enqueue(async () => await waitFlag.WaitForRaised());
        workQueue.Enqueue(async () => await waitFlag.WaitForRaised());
        
        workQueue.Enqueue(() =>
        {
            executedFlag.Raise();
            return Task.CompletedTask;
        });

        await Task.Delay(100);
        executedFlag.IsRaised.ShouldBeFalse();
        
        waitFlag.Raise();
        await BusyWait.UntilAsync(() => executedFlag.IsRaised);
    }
    
    [TestMethod]
    public async Task StressTest()
    {
        const int testSize = 20;
        var workQueue = new WorkQueue(maxParallelism: 2);
        
        var flags = Enumerable.Repeat(new SyncedFlag(), testSize).ToArray();

        for (var i = 0; i < testSize; i++)
        {
            var j = i;
            workQueue.Enqueue(() =>
            {
                flags[j].Raise();
                return Task.CompletedTask;
            });
        }

        foreach (var flag in flags)
            await BusyWait.UntilAsync(() => flag.IsRaised);
    }
    
    [TestMethod]
    public async Task ThrownExceptionDoesNotAffectExecution()
    {
        var workQueue = new WorkQueue(maxParallelism: 1);
        var flag = new SyncedFlag();
        workQueue.Enqueue(() => Task.FromException(new TimeoutException()));
        
        workQueue.Enqueue(() =>
        {
            flag.Raise();
            return Task.CompletedTask;
        });

        await BusyWait.UntilAsync(() => flag.Position == FlagPosition.Raised);
    }
}