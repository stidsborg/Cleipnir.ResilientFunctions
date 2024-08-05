using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class TestSource : Source
{
    public TestSource(IRegisteredTimeouts? timeoutProvider = null, SyncStore? syncStore = null) : base(
        timeoutProvider ?? NoOpRegisteredTimeouts.Instance,
        syncStore: syncStore ?? (_ => new InterruptCount(0).ToTask()),
        defaultDelay: TimeSpan.FromMilliseconds(10), 
        defaultMaxWait: TimeSpan.MaxValue,
        isWorkflowRunning: () => true,
        initialSyncPerformed: () => true
    ) {}
}