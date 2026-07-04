using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
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

public class FlowExecutionState
{
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _suspendedTcs = new();
    private readonly IMessageClearer? _messageClearer;

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

    internal FlowExecutionState(
        StoredId id,
        int subflows,
        int waitingSubflows,
        FlowTimeouts timeouts,
        Task completed,
        IMessageClearer? messageClearer = null)
    {
        Id = id;
        Subflows = subflows;
        WaitingSubflows = waitingSubflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
        _messageClearer = messageClearer;

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

    public Task Push(IReadOnlyList<StoredMessage> messages)
    {
        var queueManager = QueueManager;
        if (Suspended || queueManager == null)
        {
            // The push cannot be delivered (the flow lost the suspend race, or its queue manager is not attached
            // yet), but the MessageWatchdog already marked the positions as pushed. Reopen them so they are
            // re-fetched and delivered later instead of being stranded in the ignore-set - which would make a
            // later-positioned duplicate consume the idempotency key first.
            _messageClearer?.ReopenPositions(messages.Select(m => m.Position));
            return Task.CompletedTask;
        }

        return queueManager.Push(messages);
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
