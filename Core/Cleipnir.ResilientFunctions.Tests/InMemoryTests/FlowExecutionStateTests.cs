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
    private static FlowExecutionState CreateState() => new(
        new StoredId(Guid.NewGuid()),
        subflows: 1,
        waitingSubflows: 0,
        new FlowTimeouts(),
        completed: new TaskCompletionSource().Task,
        maxWait: TimeSpan.Zero
    );

    [TestMethod]
    public async Task CommittedResolutionBlocksSuspensionUntilWaiterHasResumed()
    {
        var state = CreateState();

        state.TryResolve(() => { }).ShouldBeTrue(); //a delivery commits before the subflow declares its wait
        state.SubflowWaiting(); //arms the zero-max-wait suspension timer

        await Task.Delay(100); //give the armed suspension timer every chance to fire

        state.Suspended.ShouldBeFalse(); //the pending wake-up must block suspension

        state.ResumeResolvedSubflow();

        //with the wake-up consumed the flow is again suspendable once fully waiting
        state.SubflowWaiting();
        await BusyWait.Until(() => state.Suspended);
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
