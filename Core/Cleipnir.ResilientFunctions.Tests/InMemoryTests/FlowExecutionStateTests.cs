using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class FlowExecutionStateTests
{
    private static FlowExecutionState CreateState(TimeSpan? maxWait = null) => new(
        new StoredId(Guid.NewGuid()),
        subflows: 1,
        waitingSubflows: 0,
        new FlowTimeouts(),
        completed: new TaskCompletionSource().Task,
        maxWait: maxWait ?? TimeSpan.Zero
    );

    [TestMethod]
    public async Task SubflowResolvedBeforeParkingNeverEntersTheWaitingState()
    {
        var state = CreateState();

        state.TryResolve(() => { }).ShouldBeTrue(); //a delivery commits before the subflow declares its wait

        //the owner observes the resolution while entering the waiting state - and must not enter it
        state.TryEnterWaiting(markWaitingUnlessResolved: () => false).ShouldBeFalse();
        state.WaitingSubflows.ShouldBe(0);

        await Task.Delay(100); //the suspension timer was never armed - the flow is not fully waiting

        state.Suspended.ShouldBeFalse();

        //the subflow processes its message and waits again - now the flow is fully waiting and suspendable
        state.SubflowWaiting();
        await BusyWait.Until(() => state.Suspended);
    }

    [TestMethod]
    public void ResolutionMarksTheParkedSubflowRunningAtCommit()
    {
        var state = CreateState(maxWait: TimeSpan.FromMinutes(1)); //the suspension timer never fires in-test

        state.TryEnterWaiting(markWaitingUnlessResolved: () => true).ShouldBeTrue();
        state.WaitingSubflows.ShouldBe(1);

        //the commit performs the resume accounting itself - before any waiter thread has run
        state.TryResolve(() => state.ResumeResolvedSubflow()).ShouldBeTrue();
        state.WaitingSubflows.ShouldBe(0);
    }

    [TestMethod]
    public async Task ResolutionIsRejectedAfterSuspensionAndLateWaiterParksForever()
    {
        var state = CreateState();

        state.SubflowWaiting();
        await BusyWait.Until(() => state.Suspended);

        state.TryResolve(() => throw new Exception("resolution must not run")).ShouldBeFalse();
        state.ResumeSubflow().IsCompleted.ShouldBeFalse();
    }
}
