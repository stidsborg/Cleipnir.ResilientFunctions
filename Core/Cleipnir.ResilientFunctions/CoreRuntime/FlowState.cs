using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowState
{
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _suspendedTcs = new();

    public StoredId Id { get; }
    public int Subflows { get; private set; }
    public int WaitingSubflows { get; private set; }
    public FlowTimeouts Timeouts { get; }
    public AsyncSignal InterruptSignal { get; } = new();
    public bool Suspended { get; private set; }
    public Task SuspendedTask { get; }

    public FlowState(
        StoredId id,
        int subflows,
        int waitingSubflows,
        FlowTimeouts timeouts)
    {
        Id = id;
        Subflows = subflows;
        WaitingSubflows = waitingSubflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
    }

    public void SubflowStarted()
    {
        lock (_lock)
            Subflows++;
    }

    public void SubflowCompleted()
    {
        lock (_lock)
            Subflows--;
    }

    public void SubflowWaiting()
    {
        lock (_lock)
            WaitingSubflows++;
    }

    public bool ResumeSubflow()
    {
        lock (_lock)
            if (Suspended)
                return false;
            else
                WaitingSubflows--;

        return true;
    }

    public void Interrupt()
    {
        lock (_lock)
            if (Suspended)
                return;
            else
                WaitingSubflows = 0;
        
        InterruptSignal.Fire();
    }

    public bool Suspend()
    {
        lock (_lock)
            if (Subflows == WaitingSubflows)
                Suspended = true;
            else
                return false;
        
        _suspendedTcs.TrySetResult();
        return true;
    }
}
