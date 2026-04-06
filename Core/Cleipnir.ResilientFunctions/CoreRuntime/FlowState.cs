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
    public Action SignalSuspendedToInvoker { get; }
    public QueueManager QueueManager { get; }
    public int Threads { get; private set; }
    public int SuspendedThreads { get; private set; }
    public FlowTimeouts Timeouts { get; }
    public AsyncSignal InterruptSignal { get; } = new();
    public bool Suspended { get; private set; }

    public FlowState(
        StoredId id,
        Action signalSuspendedToInvoker,
        QueueManager queueManager,
        int threads,
        int suspendedThreads,
        FlowTimeouts timeouts)
    {
        Id = id;
        SignalSuspendedToInvoker = signalSuspendedToInvoker;
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

    public void ThreadSuspended()
    {
        lock (_lock)
            SuspendedThreads++;
    }

    public bool ResumeThread()
    {
        lock (_lock)
            if (Suspended)
                return false;
            else
                SuspendedThreads--;

        return true;
    }

    public void Interrupt()
    {
        lock (_lock)
        {
            if (Suspended)
                return;
            
            SuspendedThreads = 0;
        }
        
        InterruptSignal.Fire();
    }

    public bool Suspend()
    {
        lock (_lock)
            return Threads == SuspendedThreads && (Suspended = true);
    }
}
