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
            Subflows++;
    }

    public void SubflowCompleted()
    {
        lock (_lock)
            Subflows--;

        // The completed subflow may have been the last one running - the two transitions towards
        // Subflows == WaitingSubflows (a subflow completing or starting to wait) each check afterwards,
        // so every entry into the fully-waiting state is observed by whoever caused it.
        ArmSuspensionTimerIfFullyWaiting();
    }

    public void SubflowWaiting()
    {
        lock (_lock)
            WaitingSubflows++;

        ArmSuspensionTimerIfFullyWaiting();
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

        // Only removed after passing the resume gate: a suspension overtaking the wake-up must still find the
        // timeout registered, so it postpones to it instead of suspending without any way to be woken again.
        Timeouts.RemoveTimeout(timeoutId);
    }

    /// <summary>
    /// Runs the provided resolution (waking a waiting subflow with its result) atomically with respect to the
    /// suspension decision: once the flow has decided to suspend nothing may be resumed, so the resolution is
    /// rejected.
    /// </summary>
    public bool TryResolve(Action resolution)
    {
        lock (_lock)
        {
            if (Suspended)
                return false;

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
    // Suspension is always safe whenever the flow is fully waiting: every waiting subflow's wake-up trigger
    // (registered timeout or message) outlives the suspension decision, so the flow can always be restarted.
    private void ArmSuspensionTimerIfFullyWaiting()
    {
        lock (_lock)
            if (Subflows != WaitingSubflows || Suspended)
                return;

        _ = Task.Delay(_maxWait).ContinueWith(_ => TrySuspend());
    }

    private void TrySuspend()
    {
        lock (_lock)
        {
            if (Subflows != WaitingSubflows || Suspended || _status == FlowStatus.Completed)
                return;

            Suspended = true;
            _status = FlowStatus.Suspending;
        }

        _suspendedTcs.TrySetResult();
    }
}
