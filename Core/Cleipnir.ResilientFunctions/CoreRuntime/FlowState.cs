using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public enum FlowStatus
{
    Running = 0,
    Waiting = 1,
    Completed = 2
}

public class FlowState
{
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _suspendedTcs = new();

    public StoredId Id { get; }
    public int Subflows { get; private set; }
    public int WaitingSubflows { get; private set; }
    public FlowTimeouts Timeouts { get; }
    internal QueueManager? QueueManager { get; set; }
    public bool Suspended { get; private set; }
    public Task SuspendedTask { get; }

    private FlowStatus _status = FlowStatus.Running;
    public FlowStatus Status
    {
        get
        {
            lock (_lock)
                return _status;
        }
        set
        {
            lock (_lock)
                if (_status != FlowStatus.Completed)
                    _status = value;
        }
    }

    public FlowState(
        StoredId id,
        int subflows,
        int waitingSubflows,
        FlowTimeouts timeouts,
        Task completed)
    {
        Id = id;
        Subflows = subflows;
        WaitingSubflows = waitingSubflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;

        _ = completed.ContinueWith(_ => Status = FlowStatus.Completed);
    }

    public bool Waiting()
    {
        lock (_lock)
            return Subflows == WaitingSubflows;
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

    public Task ResumeSubflow()
    {
        lock (_lock)
            if (Suspended)
                return ForeverTask.Instance;
            else
                WaitingSubflows--;

        return Task.CompletedTask;
    }

    public void Interrupt()
    {
        if (Suspended) return;
        QueueManager?.Interrupt();
    }

    public Task Push(IReadOnlyList<StoredMessage> messages)
    {
        if (Suspended) return Task.CompletedTask;
        return QueueManager?.Push(messages) ?? Task.CompletedTask;
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
