using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public enum FlowStatus
{
    Running = 0,
    Suspending = 1,
    Completed = 2
}

public class FlowExecutionState
{
    // Task.Delay's upper bound - longer waits sleep in steps and re-check.
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromMilliseconds(int.MaxValue);

    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _suspendedTcs = new();
    private readonly IMessageClearer? _messageClearer;
    private readonly TimeSpan _maxWait;
    private int _epoch;

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
        TimeSpan maxWait = default,
        IMessageClearer? messageClearer = null)
    {
        Id = id;
        Subflows = subflows;
        WaitingSubflows = waitingSubflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
        _maxWait = maxWait;
        _messageClearer = messageClearer;

        _ = completed.ContinueWith(_ => Status = FlowStatus.Completed);
    }

    public void SubflowStarted()
    {
        lock (_lock)
        {
            Subflows++;
            _epoch++;
        }
    }

    public void SubflowCompleted()
    {
        var epoch = -1;
        lock (_lock)
        {
            Subflows--;
            if (Subflows == WaitingSubflows && !Suspended)
                epoch = _epoch;
        }
        if (epoch != -1)
            ArmSuspensionTimer(epoch);
    }

    public void SubflowWaiting()
    {
        var epoch = -1;
        lock (_lock)
        {
            WaitingSubflows++;
            if (Subflows == WaitingSubflows && !Suspended)
                epoch = _epoch;
        }
        if (epoch != -1)
            ArmSuspensionTimer(epoch);
    }

    public Task ResumeSubflow()
    {
        lock (_lock)
            if (Suspended)
                return ForeverTask.Instance;
            else
            {
                WaitingSubflows--;
                _epoch++;
            }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Waits until the provided expiry - or parks forever if the flow suspends first. The timeout is registered
    /// so it becomes the postpone-until target should the flow suspend while waiting.
    /// </summary>
    public async Task WaitUntil(EffectId timeoutId, DateTime expiry, UtcNow utcNow)
    {
        Timeouts.AddTimeout(timeoutId, expiry);
        SubflowWaiting();

        // Sleeps until expiry, woken early when the flow suspends. Looped only because Task.Delay rejects
        // spans beyond MaxDelayStep - distant expiries sleep in steps.
        while (!SuspendedTask.IsCompleted && utcNow() < expiry)
        {
            var delay = expiry - utcNow();
            await Task.WhenAny(Task.Delay(delay < MaxDelayStep ? delay : MaxDelayStep), SuspendedTask);
        }

        await ResumeSubflow(); //parks forever when the flow suspended while waiting

        Timeouts.RemoveTimeout(timeoutId);
    }

    /// <summary>
    /// Runs the provided resolution (waking a waiting subflow with its result) atomically with respect to the
    /// suspension decision: once the flow has decided to suspend nothing may be resumed, so the resolution is
    /// rejected - and a resolution that runs invalidates any armed suspension attempt, since the resolved
    /// subflow is about to resume.
    /// </summary>
    public bool TryResolve(Action resolution)
    {
        lock (_lock)
        {
            if (Suspended)
                return false;

            _epoch++;
            resolution();
        }

        return true;
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

    // Fires once the flow has been fully waiting (all subflows waiting) for the configured max-wait duration.
    // The epoch guards against stale timers: any resumed or started subflow bumps it, invalidating the attempt.
    private void ArmSuspensionTimer(int epoch)
        => _ = Task.Delay(_maxWait).ContinueWith(_ => TrySuspend(epoch));

    private void TrySuspend(int epoch)
    {
        lock (_lock)
        {
            if (_epoch != epoch || Subflows != WaitingSubflows || Suspended || _status == FlowStatus.Completed)
                return;

            Suspended = true;
            _status = FlowStatus.Suspending;
        }

        _suspendedTcs.TrySetResult();
    }
}
