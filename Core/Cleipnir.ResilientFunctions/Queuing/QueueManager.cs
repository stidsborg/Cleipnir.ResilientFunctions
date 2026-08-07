using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

public delegate bool MessagePredicate(Envelope envelope);

internal class QueueManager
{
    private const int ReservedIdPrefix = -1;
    // Internal rather than private: ExistingMessages (control-panel tooling) addresses the same reserved entries
    // when editing a flow's message state from outside the flow.
    internal static readonly EffectId DeliveredPositionsId = new([ReservedIdPrefix, 0]);
    internal static readonly EffectId IdempotencyKeysRoot   = new([ReservedIdPrefix, -1]);
    // Parent of the per-message staged-message children (admitted-but-undelivered messages).
    // [ReservedIdPrefix, 1] is retired (the removed completed-flow pending-messages blob) - do not reuse it.
    internal static readonly EffectId StagedMessagesRoot  = new([ReservedIdPrefix, 2]);

    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly ISerializer _serializer;
    private readonly TypeMapper _typeMapper;
    private readonly Effect _effect;
    private readonly FlowExecutionState _flowExecutionState;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly FlowTimeouts _timeouts;
    private readonly UtcNow _utcNow;
    private readonly IMessageClearer _messageClearer;

    // Task.Delay's upper bound - longer waits sleep in steps and re-check.
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromMilliseconds(int.MaxValue);
    private readonly IdempotencyKeys _idempotencyKeys;

    private readonly Lock _lock = new();
    private readonly List<StagedMessage> _toDeliver = new();
    private readonly List<Subscription> _subscriptions = new();
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();
    // The delivered positions whose markings the in-progress flush is persisting (copied at BeforeFlush) -
    // their store rows are deleted at AfterFlush.
    private List<long>? _positionsCoveredByFlush;

    private readonly MessageDeserializer _messageDeserializer;

    public QueueManager(
        FlowId flowId,
        StoredId storedId,
        ISerializer serializer,
        TypeMapper typeMapper,
        Effect effect,
        FlowExecutionState flowExecutionState,
        UnhandledExceptionHandler unhandledExceptionHandler,
        FlowTimeouts timeouts,
        UtcNow utcNow,
        IMessageClearer messageClearer,
        MessageDeserializer messageDeserializer,
        int maxIdempotencyKeyCount = 100,
        TimeSpan? maxIdempotencyKeyTtl = null)
    {
        _messageDeserializer = messageDeserializer;
        _flowId = flowId;
        _storedId = storedId;
        _serializer = serializer;
        _typeMapper = typeMapper;
        _effect = effect;
        _flowExecutionState = flowExecutionState;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _timeouts = timeouts;
        _utcNow = utcNow;
        _messageClearer = messageClearer;
        _idempotencyKeys = new IdempotencyKeys(IdempotencyKeysRoot, _effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);

        // Attach to the flow state at construction: the flow only becomes reachable for pushes
        // (FlowsManager.AddFlow) after Initialize has run, so a push can never reach an unattached - or an
        // uninitialized - queue manager.
        flowExecutionState.QueueManager = this;
    }

    /// <summary>
    /// Loads the persisted queue state - the delivered positions and staged-message children a prior
    /// incarnation left behind - into the delivery pipeline, and reconciles the restart's in-hand messages
    /// against it: an in-hand copy of a message the prior incarnation already delivered or staged is deduped
    /// by store position, the rest are staged as new arrivals. Called exactly once, by the creating invoker,
    /// immediately after construction and before the flow is made reachable: no other member has to guard
    /// against an uninitialized instance, and the in-hand staging needs no whole-batch lock or delivery
    /// attempt (no push can race it, no subscription can exist yet).
    /// </summary>
    public async Task Initialize(IReadOnlyList<IncomingMessage> inHandMessages)
    {
        _idempotencyKeys.Initialize();

        if (_effect.TryGet<List<long>>(DeliveredPositionsId, out var positions) && positions is { Count: > 0 })
        {
            // Remember the positions a previous incarnation already delivered, so an in-hand copy of such a
            // message (fetched before its store row was deleted) is deduped below rather than delivered a
            // second time. The positions also re-enter the delivered set: their still-live store rows are
            // deleted by the normal flush cycle (BeforeFlush/AfterFlush) - the delivered-marking was made
            // durable by the incarnation that wrote it, which is exactly the deletion precondition, and a row
            // already gone makes the delete a no-op. The persisted list shrinks again with the next delivery's
            // rewrite; it must not be emptied here, before the rows are provably gone.
            lock (_lock)
                foreach (var position in positions)
                {
                    _fetchedPositions.Add(position);
                    _deliveredPositions.Add(position);
                }
        }

        // Re-stage the staged-message children a prior incarnation left behind: each message it had staged
        // but not yet delivered persists as its own child effect. A delivered message can never appear here:
        // the delivery commit clears the child in the same pending-change batch as the delivered-position
        // marking, so no flush can persist one without the other. A child message has already passed the
        // admission gate (it was written in the same upsert as the key that admitted it), so it is staged
        // directly rather than via ProcessMessages - re-checking its idempotency key would only dedup it
        // against its own entry.
        foreach (var childId in _effect.GetChildren(StagedMessagesRoot))
        {
            var message = PendingMessages.DecodeMessage(_effect.Get<byte[]>(childId), _storedId);

            // Dead lettered on deserialization failure like any other arrival; the child carrier is cleared
            // alongside the dlq move so the message is not re-staged - and re-dead-lettered - on every restart.
            var incomingMessage = await _messageDeserializer.DeserializeOrDeadLetter(message);
            if (incomingMessage is null)
            {
                _effect.FlushlessClear(childId);
                continue;
            }

            var stagedMessage = new StagedMessage(
                //never empty: the deserializer produced it from an actual payload
                new Envelope(incomingMessage.Content!, message.Receiver, message.Sender),
                incomingMessage.Position,
                childId,
                message.MessageContent,
                message.MessageType!.Value,
                message.Receiver,
                message.Sender
            );
            lock (_lock)
                Stage(stagedMessage);
        }

        // Reconcile the restart's in-hand messages - fetched from the store before this incarnation was
        // claimed - against the resurrected state above: a message whose position was already delivered or is
        // carried by a re-staged child is dropped by ProcessMessages' position dedup, the rest are staged as
        // new arrivals. (The batch never contains empty restart-pokes - the restart itself consumed them.)
        ProcessMessages(inHandMessages);

        _effect.RegisterQueueManager(this);
    }

    public QueueClient CreateQueueClient() => new(this, _serializer, _typeMapper, _utcNow);

    /// <summary>
    /// Pushes messages fetched by the MessageWatchdog straight into the delivery pipeline, avoiding a per-flow
    /// re-fetch. (The restart's in-hand messages enter through <see cref="Initialize"/> instead, which
    /// reconciles them against the prior incarnation's queue state.) The messages were deserialized - and the
    /// undeserializable ones dead lettered - at the fetch boundary (MessageWatchdog), so every message here
    /// is deliverable - except empty (restart-poke) messages, whose positions are reopened for a later restart
    /// to consume (see ProcessMessages).
    ///
    /// Invariant on return: every handled message has been added to the flow's effect state - in memory only,
    /// deliberately unflushed for performance; it is persisted with the flow's next flush, after which the store
    /// row is deleted (<see cref="AfterFlush"/>) - and every subscription the batch resolved has had its subflow
    /// marked running: the delivery commit performs the resume accounting on the waiter's behalf
    /// (FlowExecutionState.ResumeResolvedSubflow), so the waiting-subflow accounting reflects the deliveries
    /// synchronously, with no waiting required here. The flow cannot suspend while the push is running: a
    /// suspension falling due mid-push is deferred to the push's drain (FlowExecutionState.TrySuspend), so an
    /// accepted push always completes against a live flow. Within a replica the watchdog's ignore-set makes
    /// each position reach here at most once; the one duplicate source left - a crashed replica's taken-over
    /// row racing a message-less restart that re-staged the same message from its child effect - is absorbed
    /// by ProcessMessages' position dedup.
    /// </summary>
    public void Push(IReadOnlyList<IncomingMessage> messages)
    {
        if (messages.Count == 0)
            return;

        // The whole batch - admission and delivery - runs under _lock, serializing batches against each other
        // and against delivery, subscription changes and flush snapshots. A plain lock held for the duration
        // suffices: the entire pipeline is synchronous, in-memory work - staging cannot fail and delivery
        // commits are CPU-bound - so the lock is never held across I/O.
        lock (_lock)
            try
            {
                ProcessMessages(messages);
            }
            finally
            {
                DeliverMessages();
            }
    }

    public async Task<Envelope?> Subscribe(
        MessagePredicate predicate,
        DateTime? timeout,
        EffectId messageId,
        Func<StagedMessage?, IEnumerable<EffectResult>> captureMessage)
    {
        var subscription = new Subscription(messageId, predicate, captureMessage);
        lock (_lock)
            _subscriptions.Add(subscription);

        DeliverMessages();
        // Resolved before ever waiting - the waiting accounting was never touched, so there is nothing to resume.
        if (subscription.Tcs.Task.IsCompleted)
            return (await subscription.Tcs.Task)?.Envelope;

        if (timeout != null && _utcNow() >= timeout.Value)
        {
            // The user-timeout has already expired (e.g. replay after a suspension) - resolve with no message
            // immediately instead of waiting.
            bool removed;
            lock (_lock)
                removed = _subscriptions.Remove(subscription);
            if (!removed) //a delivery won the race - again before the subflow ever entered the waiting state
                return (await subscription.Tcs.Task)?.Envelope;

            _effect.FlushlessUpserts(subscription.CaptureMessage(null));
            return null;
        }

        var delayCts = new CancellationTokenSource();
        try
        {
            if (timeout != null)
            {
                _timeouts.AddTimeout(messageId, timeout.Value);
                ArmSubscriptionTimeout(subscription, timeout.Value, delayCts.Token);
            }

            // Enter the waiting state atomically with the resolution commits: a commit that sees the mark
            // performs the resume accounting itself, and a subscription resolved between the delivery attempt
            // above and this point never enters the waiting state at all.
            var waiting = _flowExecutionState.TryEnterWaiting(markWaitingUnlessResolved: () =>
            {
                if (subscription.Tcs.Task.IsCompleted)
                    return false;
                subscription.OwnerWaiting = true;
                return true;
            });
            if (!waiting)
                return (await subscription.Tcs.Task)?.Envelope;

            // Completes only via a committed TryResolve resolution (delivery, expiry or failure) - a flow that
            // decided to suspend first leaves the task unresolved and this thread parked forever. The commit has
            // already marked this subflow running, so no accounting remains to be done here.
            var msgData = await subscription.Tcs.Task;
            return msgData?.Envelope;
        }
        finally
        {
            // Safe to remove eagerly: an unresolved waiter never reaches this point (it stays parked), and a
            // resolved one was accounted running at its commit - a suspension can no longer overtake the
            // wake-up and depend on this timeout as its postpone-until target.
            if (timeout != null)
                _timeouts.RemoveTimeout(messageId);

            await delayCts.CancelAsync();
            delayCts.Dispose();
        }
    }

    // Caller must exclude concurrent pushes - Push holds _lock for the whole batch; Initialize runs before
    // the flow is reachable, which is vacuously exclusive. Dedups by idempotency-key and by already-fetched
    // position, and stages messages for delivery.
    // Deserialization already happened at the pipeline boundary (MessageDeserializer), which dead lettered the
    // messages that failed it - and the serializer is trusted not to throw on serialization (shielded via
    // decoration, see ErrorHandlingDecorator), so staging is in-memory bookkeeping that cannot fail; an
    // exception escaping here is a framework bug and propagates to the caller.
    private void ProcessMessages(IReadOnlyList<IncomingMessage> messages)
    {
        foreach (var message in messages)
        {
            var (_, content, position, idempotencyKey, sender, receiver) = message;
            // Empty restart-pokes carry nothing to deliver and may only be consumed by an actual restart. This
            // flow is live (it accepted the push), so no restart happens now - and the row may not be deleted
            // either: the flow could suspend right after, and the append's restart guarantee must survive that.
            // Reopen the position instead, so the poke is re-fetched and consumed by a restart once the flow
            // leaves the live set. (Restart in-hand batches contain no pokes - the restart itself consumed them.)
            if (content is null)
            {
                _messageClearer.ReopenPositions([position!.Value]);
                continue;
            }

            // Store-position dedup, for the two places a store row can meet a durable record of the same
            // message: the in-hand reconciliation (Initialize) - the restart's batch may contain rows the prior
            // incarnation already delivered or staged - and, on the push path, a crashed replica's taken-over
            // row racing a message-less restart (PostponedWatchdog) that re-staged the same message from its
            // child effect. A row-less message addresses no store row, so it has nothing to dedup against here.
            if (position is { } pushedPosition)
            {
                bool alreadyFetched;
                lock (_lock)
                    alreadyFetched = _fetchedPositions.Contains(pushedPosition);
                if (alreadyFetched)
                    continue;
            }

            var idempotencyEntry = idempotencyKey != null ? _idempotencyKeys.Reserve(idempotencyKey) : null;

            if (idempotencyKey != null && idempotencyEntry is null)
            {
                // A message dropped on its idempotency key never reached staging, so it has no carrier to prune -
                // marking its store row delivered is all the bookkeeping there is.
                lock (_lock)
                    RecordDeliveredStoreRow(position);
                continue;
            }

            // The pipeline is object-form - this is the single point where the payload is serialized, for the
            // durable carriers (the staged-message child and the delivered-message capture).
            var messageContent = _serializer.Serialize(content, content.GetType());
            var messageType = _typeMapper.GetTypeId(content.GetType());

            var envelope = new Envelope(content, receiver, sender);
            lock (_lock)
            {
                // Durably capture the message as its own child effect the moment it is staged; it is
                // deleted again when the message is delivered (the delivery commit clears it in the same batch
                // as the delivered-position marking). Flushless, so it costs no I/O and dies with an
                // equally-unflushed delivery - recovery then stays store-backed and at-least-once.
                var childId = NextStagedMessageChildId();
                var encodedMessage = PendingMessages.EncodeMessage(
                    messageContent, messageType, position, idempotencyKey, sender, receiver
                );

                // One upsert for the message and the key that admitted it: neither can become durable
                // without the other, so a recorded key always has its message behind it.
                _effect.FlushlessUpserts(
                    idempotencyEntry is null
                        ? [EffectResult.Create(childId, encodedMessage)]
                        : [EffectResult.Create(childId, encodedMessage), idempotencyEntry]
                );

                var stagedMessage = new StagedMessage(
                    envelope,
                    position,
                    childId,
                    messageContent,
                    messageType,
                    receiver,
                    sender
                );
                Stage(stagedMessage);
            }
        }
    }

    // Caller must hold _lock. Inserts the message into the delivery queue - kept in delivery order even when
    // two pushers (the MessageWatchdog poll and an initialization-time push) stage batches out of order - and
    // records its store position as fetched, so later pushes of the same row are deduped.
    private void Stage(StagedMessage stagedMessage)
    {
        var insertAt = _toDeliver.FindIndex(staged => CompareDeliveryOrder(staged, stagedMessage) > 0);
        if (insertAt == -1)
            _toDeliver.Add(stagedMessage);
        else
            _toDeliver.Insert(insertAt, stagedMessage);

        if (stagedMessage.Position is { } fetchedPosition)
            _fetchedPositions.Add(fetchedPosition);
    }

    // Task.Delay is bounded, so a distant user-timeout sleeps in steps - ExpireSubscription re-arms until due.
    private void ArmSubscriptionTimeout(Subscription subscription, DateTime timeout, CancellationToken cancellationToken)
    {
        var delay = timeout - _utcNow();
        if (delay > MaxDelayStep)
            delay = MaxDelayStep;
        _ = Task.Delay(delay.RoundUpToZero(), cancellationToken)
            .ContinueWith(_ => ExpireSubscription(subscription, timeout, cancellationToken), TaskContinuationOptions.OnlyOnRanToCompletion);
    }

    private void ExpireSubscription(Subscription subscription, DateTime timeout, CancellationToken cancellationToken)
    {
        if (_utcNow() < timeout)
        {
            ArmSubscriptionTimeout(subscription, timeout, cancellationToken); //bounded sleep-step elapsed before the timeout was due
            return;
        }

        lock (_lock)
            if (!_subscriptions.Remove(subscription)) //has the subscription been resolved
                return;

        // Sealed against the suspension decision: a flow that has decided to suspend must not have its waiter
        // woken - the parked subflow is abandoned and its still-registered timeout becomes the postpone-until
        // target (the woken subflow removes the timeout itself, after passing the resume gate).
        _flowExecutionState.TryResolve(() =>
        {
            _effect.FlushlessUpserts(subscription.CaptureMessage(null));

            // See DeliverMessages: the commit resumes the parked owner's accounting itself.
            if (subscription.OwnerWaiting)
            {
                subscription.OwnerWaiting = false;
                _flowExecutionState.ResumeResolvedSubflow();
            }

            subscription.Tcs.TrySetResult(null);
        });
    }

    /// <summary>
    /// Called by the flush, before it snapshots the pending changes. Copies the delivered-positions set as the
    /// watermark of what the flush is about to persist: every marking write happens under <c>_lock</c>, so
    /// everything in the set here was fully written - together with its delivery capture - before the flush's
    /// snapshot. A delivery landing mid-flush enters the set after this copy and simply waits one more flush.
    /// </summary>
    public void BeforeFlush()
    {
        lock (_lock)
            _positionsCoveredByFlush = _deliveredPositions.ToList();
    }

    /// <summary>
    /// Called by the flush after its store write, still under the flush lock (so BeforeFlush/AfterFlush cycles
    /// never overlap). The watermarked positions' delivered-markings are now provably durable, so their store
    /// rows are deleted outright - no inspection of effect state needed. The positions leave the in-memory set
    /// only after the rows are gone: a failed delete keeps them in the set, and the next flush retries.
    /// </summary>
    public async Task AfterFlush()
    {
        List<long>? covered;
        lock (_lock)
        {
            covered = _positionsCoveredByFlush;
            _positionsCoveredByFlush = null;
        }

        if (covered is not { Count: > 0 })
            return;

        try
        {
            await _messageClearer.Clear(covered);
            lock (_lock)
                _deliveredPositions.ExceptWith(covered);
        }
        catch (Exception exception)
        {
            _unhandledExceptionHandler.Invoke(_flowId.Type, exception);
        }
    }

    private void DeliverMessages()
    {
        lock (_lock)
        {
            for (var subscriptionIndex = 0; subscriptionIndex < _subscriptions.Count; subscriptionIndex++)
            {
                var subscription = _subscriptions[subscriptionIndex];
                for (var matchIndex = 0; matchIndex < _toDeliver.Count; matchIndex++)
                    if (subscription.Predicate(_toDeliver[matchIndex].Envelope))
                    {
                        var index = matchIndex;
                        // Sealed against the suspension decision: a suspended flow must not consume the message -
                        // it stays staged, durably carried by its child effect, and the in-flight push hands its
                        // batch to the restart path (FlowExecutionState.Push returns false), so a restarted
                        // incarnation re-stages and delivers it.
                        var delivered = _flowExecutionState.TryResolve(() =>
                        {
                            var msg = _toDeliver[index];
                            _toDeliver.RemoveAt(index);
                            // A row-less message addresses no store row, so it never enters the delivered-positions
                            // list - the child prune below is its durable record instead.
                            if (msg.Position is { } deliveredPosition)
                                _deliveredPositions.Add(deliveredPosition);
                            _subscriptions.RemoveAt(subscriptionIndex);

                            // One batch, entering the pending changes atomically: the captured message, the
                            // delivered position and the clear of the message's child carrier can never be
                            // persisted torn by a concurrent flush snapshot - so a durable child's position is
                            // never in the durable delivered list (the invariant Initialize's re-staging relies
                            // on). Flushless on purpose: dying with all three unflushed replays the message -
                            // at-least-once, exactly like a store-resident message.
                            _effect.FlushlessUpserts(
                                subscription.CaptureMessage(msg)
                                    .Append(EffectResult.Create(DeliveredPositionsId, _deliveredPositions.ToList()))
                                    .Append(EffectResult.Clear(msg.ChildId))
                            );

                            // The commit performs the resume accounting on the parked owner's behalf - a
                            // subscription resolved before its owner entered the waiting state has nothing to
                            // resume (the owner observes the result and never waits).
                            if (subscription.OwnerWaiting)
                            {
                                subscription.OwnerWaiting = false;
                                _flowExecutionState.ResumeResolvedSubflow();
                            }

                            subscription.Tcs.TrySetResult(msg);
                        });
                        if (!delivered)
                            return;

                        DeliverMessages();
                        return;
                    }
            }
        }
    }

    /// <summary>
    /// A message past the admission gate (fetched-position dedup and idempotency-key claim), staged for delivery
    /// and waiting for a matching subscription. Deliberately carries no idempotency key: the key belongs to
    /// admission, which is behind it - its durable child effect (<see cref="ChildId"/>) was written together with
    /// the key that admitted it.
    /// </summary>
    public record StagedMessage(
        Envelope Envelope,
        long? Position,
        EffectId ChildId,
        byte[] MessageContentBytes,
        TypeId MessageType,
        string? Receiver,
        string? Sender
    );

    // Delivery order: row-less messages (control-panel appended) first in child order, then store-addressed
    // messages by position. A comparison rather than a sortable pseudo-position - a row-less message genuinely has
    // no position, and any value invented for one here would have to be filtered back out of every store-facing
    // path it reached.
    private static int CompareDeliveryOrder(StagedMessage left, StagedMessage right)
    {
        if (left.Position is { } leftPosition && right.Position is { } rightPosition)
            return leftPosition.CompareTo(rightPosition);
        if (left.Position is null && right.Position is null)
            return left.ChildId.Id.CompareTo(right.ChildId.Id);

        return left.Position is null ? -1 : 1;
    }

    // Caller must hold _lock. A row-less message addresses no store row, so it leaves no delivered-position mark.
    private void RecordDeliveredStoreRow(long? position)
    {
        if (position is not { } storePosition)
            return;

        _deliveredPositions.Add(storePosition);
        _effect.FlushlessUpsert(DeliveredPositionsId, _deliveredPositions.ToList(), alias: null);
    }

    // The next free child slot (highest existing direct-child index + 1), computed without writing - the message
    // is written together with the idempotency entry that admitted it, in a single upsert.
    private EffectId NextStagedMessageChildId()
    {
        var nextIndex = 0;
        foreach (var childId in _effect.GetChildren(StagedMessagesRoot))
            if (childId.Id >= nextIndex)
                nextIndex = childId.Id + 1;

        return StagedMessagesRoot.CreateChild(nextIndex);
    }

    private record Subscription(EffectId EffectId, MessagePredicate Predicate, Func<StagedMessage?, IEnumerable<EffectResult>> CaptureMessage)
    {
        public TaskCompletionSource<StagedMessage?> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        // True while the owner is parked in the waiting state - set and consumed under the flow-state lock
        // (TryEnterWaiting / the TryResolve resolution), so a commit either sees the mark and resumes the
        // subflow itself or the owner sees the resolved result and never waits.
        public bool OwnerWaiting { get; set; }
    }
}
