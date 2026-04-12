using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowState
{
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _suspendedTcs = new();

    public StoredId Id { get; }
    public QueueManager QueueManager { get; }
    public int Threads { get; private set; }
    public int WaitingThreads { get; private set; }
    public FlowTimeouts Timeouts { get; }
    public AsyncSignal InterruptSignal { get; } = new();
    public bool Suspended { get; private set; }
    public Task SuspendedTask { get; }

    public FlowState(
        StoredId id,
        QueueManager queueManager,
        int threads,
        int waitingThreads,
        FlowTimeouts timeouts)
    {
        Id = id;
        QueueManager = queueManager;
        Threads = threads;
        WaitingThreads = waitingThreads;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
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
            WaitingThreads++;
    }

    public bool ResumeThread()
    {
        lock (_lock)
            if (Suspended)
                return false;
            else
                WaitingThreads--;

        return true;
    }

    public void Interrupt()
    {
        lock (_lock)
        {
            if (Suspended)
                return;
            
            WaitingThreads = 0;
        }
        
        InterruptSignal.Fire();
    }

    public bool Suspend()
    {
        lock (_lock)
            if (Threads == WaitingThreads)
                Suspended = true;
            else
                return false;
        
        _suspendedTcs.TrySetResult();
        return true;
    }
}
