using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class FlowState
{
    private readonly Lock _lock = new();

    public StoredId Id { get; }
    public Action Suspend { get; }
    public QueueManager QueueManager { get; }
    public int Threads { get; private set; }
    public int SuspendedThreads { get; private set; }
    public FlowTimeouts Timeouts { get; }
    public AsyncSignal Signal { get; } = new();

    public FlowState(
        StoredId id,
        Action suspend,
        QueueManager queueManager,
        int threads,
        int suspendedThreads,
        FlowTimeouts timeouts)
    {
        Id = id;
        Suspend = suspend;
        QueueManager = queueManager;
        Threads = threads;
        SuspendedThreads = suspendedThreads;
        Timeouts = timeouts;
    }

    public void NewThreadStarted()
    {
        lock (_lock)
            Threads++;
    }

    public void ThreadCompleted()
    {
        lock (_lock)
            Threads--;
    }

    public async Task ThreadSuspended()
    {
        lock (_lock)
            SuspendedThreads++;

        await Signal.Wait();
    }

    public void Interrupt()
    {
        lock (_lock)
            SuspendedThreads = 0;

        Signal.Fire();
    }
}
