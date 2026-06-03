using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
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
    private readonly TimeSpan _maxWaitBeforeSuspension;
    private DateTime? _waitingForMessageSince;

    public StoredId Id { get; }
    public int Subflows { get; private set; }
    public int WaitingSubflows { get; private set; }
    public FlowTimeouts Timeouts { get; }
    internal QueueManager? QueueManager { get; set; }
    public bool Suspended { get; private set; }
    public Task SuspendedTask { get; }

    private bool _interrupted;
    public bool Interrupted
    {
        get
        {
            lock (_lock)
                return _interrupted;
        }
        set
        {
            lock (_lock)
                _interrupted = value;
        }
    }

    private FlowStatus _status = FlowStatus.Running;
    public FlowStatus Status
    {
        get
        {
            lock (_lock)
                return _status;
        }
        private set
        {
            lock (_lock)
                if (_status != FlowStatus.Completed)
                    _status = value;
        }
    }

    public FlowState(
        StoredId id,
        int subflows,
        FlowTimeouts timeouts,
        Task completed,
        TimeSpan maxWaitBeforeSuspension)
    {
        Id = id;
        Subflows = subflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
        _maxWaitBeforeSuspension = maxWaitBeforeSuspension;

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
        => TryResumeSubflow()
            ? Task.CompletedTask
            : ForeverTask.Instance;
    
    public bool TryResumeSubflow()
    {
        lock (_lock)
            if (Suspended)
                return false;
            else
                WaitingSubflows--;

        return true;
    }

    public bool Interrupt()
    {
        if (Suspended) return false;

        QueueManager?.Interrupt();
        return true;
    }

    private bool AllSubflowWaitingForMessage()
    {
        lock (_lock)
            if (QueueManager == null)
                return false;
            else
                return QueueManager.SubscribtionsCount() == Subflows;
    }
    
    public bool TrySuspend()
    {
        lock (_lock)
            if (AllSubflowWaitingForMessage())
                Suspended = true;
            else
                return false;

        _suspendedTcs.TrySetResult();
        return true;
    }

    public bool TrySuspendIfMaxWaitExceeded(DateTime now)
    {
        lock (_lock)
        {
            if (Suspended)
                return false;

            if (!AllSubflowWaitingForMessage())
            {
                _waitingForMessageSince = null;
                return false;
            }

            _waitingForMessageSince ??= now;
            if (now - _waitingForMessageSince.Value < _maxWaitBeforeSuspension)
                return false;
        }

        return TrySuspend();
    }
}
