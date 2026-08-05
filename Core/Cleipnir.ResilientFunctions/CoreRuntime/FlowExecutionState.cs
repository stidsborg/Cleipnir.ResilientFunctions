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
    /// Enters the waiting state unless the provided check - evaluated under the state lock, atomically with
    /// resolution commits (<see cref="TryResolve"/>) - reports the subscription already resolved. This closes
    /// the pre-park race by construction: either the commit observed the waiting mark and resumed the subflow
    /// itself, or the owner observes the resolved result here and never enters the waiting state at all.
    /// Returns false when the subscription was already resolved.
    /// </summary>
    public bool TryEnterWaiting(Func<bool> markWaitingUnlessResolved)
    {
        lock (_lock)
        {
            if (!markWaitingUnlessResolved())
                return false;

            WaitingSubflows++;
        }

        ArmSuspensionTimerIfFullyWaiting();
        return true;
    }

    /// <summary>
    /// Runs the provided resolution (waking a waiting subflow with its result) atomically with respect to the
    /// suspension decision: once the flow has decided to suspend nothing may be resumed, so the resolution is
    /// rejected. A committed resolution performs the resume accounting itself
    /// (<see cref="ResumeResolvedSubflow"/>, for an owner that entered the waiting state) - synchronously,
    /// under the same lock - so the suspension timer can never observe an already-delivered message's subflow
    /// as still waiting.
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

    /// <summary>
    /// Marks a resolved subscription's waiting subflow as running. Called by the resolution itself - inside
    /// <see cref="TryResolve"/>, at the commit point - so the waiting accounting reflects the delivery before
    /// the resolving call (e.g. a push's delivery loop) returns; the physically parked waiter resumes later and
    /// performs no accounting of its own. Unlike <see cref="ResumeSubflow"/> this never parks: the commit is
    /// sealed against suspension, and afterwards the subflow counts as running, so the flow cannot suspend
    /// until it waits again.
    /// </summary>
    public void ResumeResolvedSubflow()
    {
        lock (_lock)
            WaitingSubflows--;
    }

    /// <summary>
    /// Routes the pushed messages to the attached queue manager. Returns true when the flow accepted the push.
    /// Otherwise the flow does not accept it - it has decided to suspend (observable only at entry: a suspension
    /// due mid-push is deferred to the push's drain, so an accepted push always runs to completion on a live
    /// flow) or its invocation is ending (<see cref="ClosePushes"/>) - and the caller must hand the messages to
    /// the restart path (awaiting <see cref="Completed"/> first). Safe to re-hand wholesale: dead lettering
    /// happened at the fetch boundary, before the pipeline, and the restarted incarnation reconciles the batch
    /// against the queue state it resurrects (QueueManager.Initialize) - whatever a completed-but-refused push
    /// already staged was persisted by this incarnation's final flush and is deduped there, not staged twice.
    /// </summary>
    internal bool Push(IReadOnlyList<IncomingMessage> messages)
    {
        lock (_lock)
        {
            if (Suspended || _pushesClosed)
                return false;
            _activePushes++;
        }

        //never null: the flow only becomes reachable (FlowsManager.AddFlow) after the queue manager is attached
        QueueManager!.Push(messages);

        bool accepted;
        bool retryDeferredSuspend;
        lock (_lock)
        {
            _activePushes--;
            if (_pushesClosed && _activePushes == 0)
                _pushesDrainedTcs.TrySetResult();

            // A suspension falling due mid-push is deferred (TrySuspend's push invariant) and can only commit
            // after this lock is released - so only teardown can refuse a completed push, never suspension.
            // Decided atomically with the drain, and refusal strands nothing: the completed push's deliveries
            // have already marked their subflows running and its staged messages are persisted (as child
            // effects) by the ending invocation's final flush.
            accepted = !_pushesClosed;

            retryDeferredSuspend = _suspendDeferredByPush && _activePushes == 0;
            if (retryDeferredSuspend)
                _suspendDeferredByPush = false;
        }

        if (retryDeferredSuspend)
            TrySuspend();

        return accepted;
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
    // Suspension is safe whenever the flow is fully waiting: every still-waiting subflow's wake-up trigger
    // (registered timeout or message) outlives the suspension decision, so the flow can always be restarted. A
    // resolved subscription can never be observed as waiting - its commit marked the subflow running
    // (ResumeResolvedSubflow) under the same lock the suspension decision takes.
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
