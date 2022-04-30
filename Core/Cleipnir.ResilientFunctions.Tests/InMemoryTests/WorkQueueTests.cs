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
        var workItem = new WorkQueue.WorkItem(
            "id1",
            () =>
            {
                flag.Raise();
                return Task.CompletedTask;
            }
        );
        workQueue.Enqueue(new [] {workItem});

        await BusyWait.UntilAsync(() => flag.Position == FlagPosition.Raised);
    }
    
    [TestMethod]
    public async Task EnqueuedWorkIsOnlyExecutedAfterPreviousWorkHasCompleted()
    {
        var workQueue = new WorkQueue(maxParallelism: 2);
        var waitFlag = new SyncedFlag();
        var executedFlag = new SyncedFlag();
        var workItems = new WorkQueue.WorkItem[]
        {
            new ("id1", async () => await waitFlag.WaitForRaised()),
            new ("id2", async () => await waitFlag.WaitForRaised())
        };

        workQueue.Enqueue(workItems);

        workQueue.Enqueue(new WorkQueue.WorkItem[]
        {
            new("id3", () =>
            {
                executedFlag.Raise();
                return Task.CompletedTask;
            })
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
        var workItems = Enumerable
            .Range(0, testSize)
            .Select(i => new WorkQueue.WorkItem(
                $"id{i}", () =>
                {
                    flags[i].Raise();
                    return Task.CompletedTask;
                })
            );
        
        workQueue.Enqueue(workItems);

        foreach (var flag in flags)
            await BusyWait.UntilAsync(() => flag.IsRaised);
    }
    
    [TestMethod]
    public async Task ThrownExceptionDoesNotAffectExecution()
    {
        var workQueue = new WorkQueue(maxParallelism: 1);
        var flag = new SyncedFlag();
        workQueue.Enqueue(new []
        {
            new WorkQueue.WorkItem("id1", () => Task.FromException(new TimeoutException())),
            new WorkQueue.WorkItem("id2", () => { flag.Raise(); return Task.CompletedTask; })
        });

        await BusyWait.UntilAsync(() => flag.Position == FlagPosition.Raised);
    }
    
    [TestMethod]
    public async Task SameIdWorkIsOnlyExecutedOnce()
    {
        var workQueue = new WorkQueue(maxParallelism: 1);
        var waitFlag = new SyncedFlag();
        var counter = new SyncedCounter();
        
        workQueue.Enqueue(new WorkQueue.WorkItem[]
        {
            new("id0", async () => { await waitFlag.WaitForRaised(); })   
        });
        
        workQueue.Enqueue(new WorkQueue.WorkItem[]
        {
            new("id1", () => { counter.Increment(); return Task.CompletedTask; })   
        });
        
        workQueue.Enqueue(new WorkQueue.WorkItem[]
        {
            new("id1", () => { counter.Increment(); return Task.CompletedTask; })   
        });
        
        waitFlag.Raise();

        await Task.Delay(10);
        
        counter.Current.ShouldBe(1);
    }
}