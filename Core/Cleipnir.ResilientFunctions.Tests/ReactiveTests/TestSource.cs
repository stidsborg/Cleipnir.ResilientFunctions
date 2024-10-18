using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class TestSource : Source
{
    public TestSource(IRegisteredTimeouts? registeredTimeouts = null, SyncStore? syncStore = null) : base(
        registeredTimeouts ?? NoOpRegisteredTimeouts.Instance,
        syncStore: syncStore ?? (_ => Task.CompletedTask),
        defaultDelay: TimeSpan.FromMilliseconds(10), 
        defaultMaxWait: TimeSpan.MaxValue,
        isWorkflowRunning: () => true,
        initialSyncPerformed: () => true
    ) {}
}