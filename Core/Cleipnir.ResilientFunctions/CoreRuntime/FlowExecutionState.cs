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
    private readonly TimeSpan _maxWait;
    // Resolutions committed through TryResolve whose woken waiter has not yet acted on them. The waiter still
    // counts as waiting until it passes the resume gate, so this is what blocks TrySuspend from suspending away
    // an already-consumed wake-up (the message would be durably delivered while the flow never processes it).
    private int _pendingWakeups;
    // Completes the pending WhenWakeupsConsumed waiters when _pendingWakeups drops to zero. Allocated lazily -
    // only a push that actually resolved subscriptions ever waits.
    private TaskCompletionSource? _wakeupsConsumedTcs;
    // Pushes currently inside the queue manager. ClosePushes waits for them to drain before the invocation's
    // final persistence, so everything a push stages is included in the incarnation's last flush.
    private int _activePushes;
    private bool _pushesClosed;
    // Set when a due suspension was refused solely because a push was in flight - the last draining push
    // retries it, since no waiting-state transition (the normal re-arm trigger) may ever come.
    private bool _suspendDeferredByPush;
    private readonly TaskCompletionSource _pushesDrainedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public StoredId Id { get; }
    public int Subflows { get; private set; }
    public int WaitingSubflows { get; private set; }
    public FlowTimeouts Timeouts { get; }
    internal QueueManager? QueueManager { get; set; }
    public bool Suspended { get; private set; }
    public Task SuspendedTask { get; }
    // Completes - never faults - when the invocation has ended for any outcome; for suspension that is after
    // the Suspended/Postponed status has been persisted and the incarnation is restartable.
    public Task Completed { get; }

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
        TimeSpan maxWait = default)
    {
        Id = id;
        Subflows = subflows;
        WaitingSubflows = waitingSubflows;
        Timeouts = timeouts;
        SuspendedTask = _suspendedTcs.Task;
        _maxWait = maxWait;

        Completed = completed.ContinueWith(_ => Status = FlowStatus.Completed);
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
    /// rejected. A committed resolution registers a pending wake-up, which blocks suspension until the woken
    /// waiter has consumed it (<see cref="ResumeResolvedSubflow"/> / <see cref="WakeupConsumed"/>) - otherwise
    /// the suspension timer could still observe the waiter as waiting and suspend away an already-delivered
    /// message.
    /// </summary>
    public bool TryResolve(Action resolution)
    {
        lock (_lock)
        {
            if (Suspended)
                return false;

            resolution();
            _pendingWakeups++;
        }

        return true;
    }

    /// <summary>
    /// Consumes a committed resolution's pending wake-up for a subflow that was never parked (it observed the
    /// resolved result before declaring itself waiting).
    /// </summary>
    public void WakeupConsumed()
    {
        lock (_lock)
        {
            _pendingWakeups--;
            SignalIfAllWakeupsConsumed();
        }
    }

    /// <summary>
    /// Resumes a subflow woken by a committed resolution: leaves the waiting state and consumes the pending
    /// wake-up in one step. Unlike <see cref="ResumeSubflow"/> this never parks - the pending wake-up has
    /// blocked suspension since the resolution committed, so the flow cannot have suspended in between.
    /// </summary>
    public void ResumeResolvedSubflow()
    {
        lock (_lock)
        {
            WaitingSubflows--;
            _pendingWakeups--;
            SignalIfAllWakeupsConsumed();
        }
    }

    // Caller must hold _lock.
    private void SignalIfAllWakeupsConsumed()
    {
        if (_pendingWakeups != 0 || _wakeupsConsumedTcs is null)
            return;

        _wakeupsConsumedTcs.TrySetResult();
        _wakeupsConsumedTcs = null;
    }

    /// <summary>
    /// Completes once every committed wake-up has been consumed - each resolved subscription's waiter has passed
    /// its resume gate. Awaited by the queue manager's push, whose invariant is that it only returns after the
    /// subscriptions it resolved have actually resumed: the flow's waiting-subflow accounting then reflects the
    /// delivery, so a suspension decision after the push observes the woken subflows as running.
    /// </summary>
    public Task WhenWakeupsConsumed()
    {
        lock (_lock)
        {
            if (_pendingWakeups == 0)
                return Task.CompletedTask;

            _wakeupsConsumedTcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _wakeupsConsumedTcs.Task;
        }
    }

    /// <summary>
    /// Routes the pushed messages to the attached queue manager. Returns null when the flow accepted the push.
    /// Otherwise the flow does not accept it - it has decided to suspend (observable only at entry: a suspension
    /// due mid-push is deferred to the push's drain, so an accepted push always runs to completion on a live
    /// flow) or its invocation is ending (<see cref="ClosePushes"/>) - and the returned messages are the ones
    /// the caller must hand to the restart path (awaiting <see cref="Completed"/> first). Messages the push dead
    /// lettered are excluded from the returned list - dead lettering is terminal and re-handing one would
    /// duplicate its dlq entry; every other handling is idempotent under the restart's re-push.
    /// </summary>
    public async Task<IReadOnlyList<StoredMessage>?> Push(IReadOnlyList<StoredMessage> messages)
    {
        lock (_lock)
        {
            if (Suspended || _pushesClosed)
                return messages;
            _activePushes++;
        }

        IReadOnlyList<long> deadLetteredPositions = [];
        var accepted = false;
        try
        {
            //never null: the flow only becomes reachable (FlowsManager.AddFlow) after the queue manager is attached
            // The push deserializes at the pipeline boundary - messages failing deserialization are dead lettered
            // there and never enter the delivery pipeline.
            deadLetteredPositions = await QueueManager!.Push(messages);
        }
        finally
        {
            bool retryDeferredSuspend;
            lock (_lock)
            {
                _activePushes--;
                if (_pushesClosed && _activePushes == 0)
                    _pushesDrainedTcs.TrySetResult();

                // Decided atomically with the drain: a suspension deferred to this push can only commit after
                // the lock is released, i.e. after the push has already been accepted - and such a suspension
                // strands nothing, since the completed push's deliveries are consumed and its staged messages
                // are persisted (as child effects) by the suspension's own flush.
                accepted = !Suspended && !_pushesClosed;

                retryDeferredSuspend = _suspendDeferredByPush && _activePushes == 0;
                if (retryDeferredSuspend)
                    _suspendDeferredByPush = false;
            }

            if (retryDeferredSuspend)
                TrySuspend();
        }

        if (accepted)
            return null;

        return deadLetteredPositions.Count == 0
            ? messages
            : messages.Where(message => !deadLetteredPositions.Contains(message.Position)).ToList();
    }

    /// <summary>
    /// Stops accepting pushes and completes once the in-flight ones have drained. Awaited by the ending
    /// invocation before its final persistence, making teardown - like every other state change - arbitrated
    /// here: no push ever spans the final flush, so everything a completed push staged is persisted with the
    /// flow. Pushes refused from then on take the restart path instead (see <see cref="Push"/>).
    /// </summary>
    public Task ClosePushes()
    {
        lock (_lock)
        {
            _pushesClosed = true;
            if (_activePushes == 0)
                _pushesDrainedTcs.TrySetResult();
        }

        return _pushesDrainedTcs.Task;
    }

    // Fires once the flow has been fully waiting (all subflows waiting) for the configured max-wait duration.
    // Suspension is safe whenever the flow is fully waiting with no pending wake-ups: every still-waiting
    // subflow's wake-up trigger (registered timeout or message) outlives the suspension decision, so the flow
    // can always be restarted. A committed-but-unconsumed resolution has no surviving trigger - its message is
    // already recorded as delivered - which is why TrySuspend refuses while one is pending.
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
            if (Subflows != WaitingSubflows || _pendingWakeups != 0 || Suspended || _status == FlowStatus.Completed)
                return;

            // The push invariant: while messages are being pushed and the subscriptions they resolve resumed,
            // the flow cannot suspend. Defer the otherwise-due suspension to the last draining push - a push
            // that changed no waiting state would never re-arm the suspension timer.
            if (_activePushes != 0)
            {
                _suspendDeferredByPush = true;
                return;
            }

            Suspended = true;
            _status = FlowStatus.Suspending;
        }

        _suspendedTcs.TrySetResult();
    }
}
