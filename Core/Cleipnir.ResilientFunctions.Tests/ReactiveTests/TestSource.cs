using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class TestSource : Source
{
    public TestSource(IRegisteredTimeouts? registeredTimeouts = null, SyncStore? syncStore = null, TimeSpan? maxWait = null) : base(
        registeredTimeouts ?? NoOpRegisteredTimeouts.Instance,
        syncStore: syncStore ?? (_ => Task.CompletedTask),
        defaultDelay: TimeSpan.FromMilliseconds(10), 
        defaultMaxWait: maxWait ?? TimeSpan.MaxValue,
        isWorkflowRunning: () => true,
        initialSyncPerformed: () => true
    ) {}
}