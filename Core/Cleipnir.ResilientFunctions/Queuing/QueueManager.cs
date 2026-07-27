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
    // Parent of the per-message staged-message children (admitted-but-undelivered messages). A dedicated id - not
    // PendingMessages.EffectId - because FlushlessClear cascades to children, and the completed-flow blob lives
    // at (and is cleared via) PendingMessages.EffectId; keeping the carriers on separate ids stops the blob's
    // clear from deleting these children.
    internal static readonly EffectId StagedMessagesRoot  = new([ReservedIdPrefix, 2]);
    // The value written whenever the delivered-positions entry is emptied. Shared rather than allocated per write:
    // the upsert serializes it eagerly and never retains the instance, and TryGet always hands back a freshly
    // deserialized list - so no caller can reach this one. Never add to it.
    private static readonly List<long> NoDeliveredPositions = new();

    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly ISerializer _serializer;
    private readonly Effect _effect;
    private readonly FlowExecutionState _flowExecutionState;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly FlowTimeouts _timeouts;
    private readonly UtcNow _utcNow;
    private readonly IMessageClearer _messageClearer;

    // Task.Delay's upper bound - longer waits sleep in steps and re-check.
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromMilliseconds(int.MaxValue);
    private readonly IdempotencyKeys _idempotencyKeys;

    private readonly SemaphoreSlim _fetchSemaphore = new(1);
    private readonly Lock _lock = new();
    private readonly List<StagedMessage> _toDeliver = new();
    private readonly List<Subscription> _subscriptions = new();
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();
    // Messages inlined into the pending-messages effect while the flow was completed - staged at initialization
    // and pruned from the durable entry as they are delivered (see PruneDeliveredMessage). Kept in the
    // entry's own (position-ascending) order, so a rewrite re-encodes the list as-is.
    private readonly List<IncomingMessage> _pendingInlinedMessages = new();
    // The delivered positions whose markings the in-progress flush is persisting (copied at BeforeFlush) -
    // their store rows are deleted at AfterFlush.
    private List<long>? _positionsCoveredByFlush;

    private readonly MessageDeserializer _messageDeserializer;

    public QueueManager(
        FlowId flowId,
        StoredId storedId,
        ISerializer serializer,
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
    /// Loads the persisted queue state - the delivered positions, staged-message children and inlined pending
    /// messages a prior incarnation left behind - into the delivery pipeline. Called exactly once, by the creating
    /// invoker, immediately after construction and before the flow is made reachable or handed any messages: no
    /// other member has to guard against an uninitialized instance.
    /// </summary>
    public async Task Initialize()
    {
        _idempotencyKeys.Initialize();

        if (_effect.TryGet<List<long>>(DeliveredPositionsId, out var positions) && positions is { Count: > 0 })
        {
            // Remember the positions a previous incarnation already delivered, so a message fetched before its
            // Clear deletes it from the store (e.g. the restart's in-hand messages) is skipped by
            // ProcessMessages rather than delivered a second time.
            lock (_lock)
                foreach (var position in positions)
                    _fetchedPositions.Add(position);

            await _messageClearer.Clear(positions);

            _effect.FlushlessUpsert(DeliveredPositionsId, NoDeliveredPositions, alias: null);
        }

        // Re-stage the staged-message children a prior incarnation left behind: each message it had staged
        // but not yet delivered persists as its own child effect. A child whose position was already
        // delivered (replayed above) is pruned rather than re-delivered - the analogue of the delivered-
        // positions store-row clear above. A child message has already passed the admission gate (it was
        // written in the same upsert as the key that admitted it), so it is staged directly rather than via
        // ProcessMessages - re-checking its idempotency key would only dedup it against its own entry.
        foreach (var childId in _effect.GetChildren(StagedMessagesRoot))
        {
            var message = PendingMessages.DecodeMessage(_effect.Get<byte[]>(childId));

            // Only a store-addressed child can have been delivered by a prior incarnation - the replayed
            // delivered positions are store positions, so a row-less message can never match one.
            if (message.Position is { } position)
            {
                bool alreadyDelivered;
                lock (_lock)
                    alreadyDelivered = _fetchedPositions.Contains(position);
                if (alreadyDelivered)
                {
                    _effect.FlushlessClear(childId);
                    continue;
                }
            }

            // Dead lettered on deserialization failure like any other arrival; the child carrier is cleared
            // alongside the dlq move so the message is not re-staged - and re-dead-lettered - on every restart.
            var payload = await _messageDeserializer.DeserializeOrDeadLetter(message);
            if (payload is null)
            {
                _effect.FlushlessClear(childId);
                continue;
            }

            var stagedMessage = new StagedMessage(
                new Envelope(payload, message.Receiver, message.Sender),
                message.Position,
                childId,
                message.MessageContent,
                message.MessageType,
                message.Receiver,
                message.Sender
            );
            lock (_lock)
                Stage(stagedMessage);
        }

        // Stage messages that were inlined into the effect state while the flow was completed (their store
        // rows are deleted, so this entry is their only carrier). ProcessMessages dedups them against the
        // replayed delivered positions and the persisted idempotency keys; running it here without the fetch
        // semaphore is safe - nothing can push before initialization has completed.
        var pendingEntry = _effect.GetStoredEffect(PendingMessages.EffectId);
        if (pendingEntry?.Result is { Length: > 0 } pendingBytes)
        {
            var pendingMessages = PendingMessages.Decode(pendingBytes);
            lock (_lock)
                _pendingInlinedMessages.AddRange(pendingMessages);

            // The blob is a dead lettered message's only carrier - prune it so the message is not re-staged - and
            // re-dead-lettered - on every restart.
            var (deserialized, deadLettered) = await _messageDeserializer.Deserialize(pendingMessages);
            lock (_lock)
                foreach (var deadLetter in deadLettered)
                    PruneDeliveredMessage(childId: null, deadLetter.Position);

            ProcessMessages(deserialized);
        }

        _effect.RegisterQueueManager(this);
    }

    public QueueClient CreateQueueClient() => new(this, _serializer, _utcNow);

    /// <summary>
    /// Pushes messages fetched elsewhere (the MessageWatchdog, or the in-hand messages handed over on restart)
    /// straight into the delivery pipeline, avoiding a per-flow re-fetch. The messages are deserialized here - at
    /// the pipeline boundary - and the undeserializable ones are dead lettered instead of pushed; empty
    /// (restart-poke) messages are stripped by both routes before handing over.
    ///
    /// Invariant on return: every handled message has been added to the flow's effect state - in memory only,
    /// deliberately unflushed for performance; it is persisted with the flow's next flush, after which the store
    /// row is deleted (<see cref="AfterFlush"/>) - and every subscription the batch resolved has had its subflow
    /// marked running: the delivery commit performs the resume accounting on the waiter's behalf
    /// (FlowExecutionState.ResumeResolvedSubflow), so the waiting-subflow accounting reflects the deliveries
    /// synchronously, with no waiting required here. The flow cannot suspend while the push is running: a
    /// suspension falling due mid-push is deferred to the push's drain (FlowExecutionState.TrySuspend), so an
    /// accepted push always completes against a live flow. Idempotent: positions already processed are skipped
    /// by ProcessMessages.
    ///
    /// Returns the dead lettered messages' store positions. Dead lettering is terminal - the message is in the
    /// dlq and its row deleted - and, unlike every other handling, NOT idempotent to redo: a caller re-handing
    /// the batch to the restart path must exclude these messages, or the dlq receives a duplicate entry.
    /// </summary>
    public async Task<IReadOnlyList<long>> Push(IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
            return [];

        var (deserialized, deadLettered) = await _messageDeserializer.Deserialize(
            messages.Select(IncomingMessage.From).ToList()
        );

        await _fetchSemaphore.WaitAsync();
        try
        {
            ProcessMessages(deserialized);
        }
        finally
        {
            DeliverMessages();
            _fetchSemaphore.Release();
        }

        return deadLettered.Count == 0
            ? []
            : deadLettered.Where(m => m.Position is not null).Select(m => m.Position!.Value).ToList();
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

    // Caller must hold _fetchSemaphore. Dedups by idempotency-key and by already-fetched position (so pushes are
    // idempotent), and stages messages for delivery. Deserialization already happened at the pipeline boundary
    // (MessageDeserializer), which dead lettered the messages that failed it - and the serializer is trusted not
    // to throw on serialization (shielded via decoration, see ErrorHandlingDecorator), so staging is in-memory
    // bookkeeping that cannot fail; an exception escaping here is a framework bug and propagates to the caller.
    private void ProcessMessages(IReadOnlyList<DeserializedMessage> messages)
    {
        foreach (var message in messages)
        {
            var (msg, position, idempotencyKey, sender, receiver) = message;

            // Push dedup is store-row dedup: only a message addressing a store row can be pushed twice, so a
            // row-less message has nothing to dedup against here.
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
                lock (_lock)
                {
                    RecordDeliveredStoreRow(position);
                    // A gate message never has a child carrier - the prune only targets the completed-flow
                    // inline blob, when the message came from there.
                    PruneDeliveredMessage(childId: null, position);
                }
                continue;
            }

            // The serialized bytes travel no further than the pipeline boundary - re-serialize the payload
            // for the durable carriers (the staged-message child and the delivered-message capture).
            var messageContent = _serializer.Serialize(msg, msg.GetType());
            var messageType = _serializer.SerializeType(msg.GetType());

            var envelope = new Envelope(msg, receiver, sender);
            lock (_lock)
            {
                // Durably capture the message as its own child effect the moment it is staged; it is
                // deleted again when the message is delivered or idempotency-deduped (PruneDeliveredMessage).
                // Flushless, so it costs no I/O and dies with an equally-unflushed delivery - recovery then stays
                // store-backed and at-least-once.
                var childId = NextStagedMessageChildId();
                var encodedMessage = PendingMessages.EncodeMessage(
                    new IncomingMessage(messageContent, messageType, position, idempotencyKey, sender, receiver)
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

                            _effect.FlushlessUpserts(
                                subscription.CaptureMessage(msg)
                                    .Append(EffectResult.Create(DeliveredPositionsId, _deliveredPositions.ToList()))
                            );
                            // Same pending-change batch as the capture above - the prune, the captured message and
                            // the delivered position land in one atomic effect write at the next flush.
                            PruneDeliveredMessage(msg.ChildId, msg.Position);

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

    // Caller must hold _lock. Removes a delivered (or idempotency-deduped) message from its durable carrier - the
    // per-message child effect (running flow) and/or the completed-flow inline blob - so a later incarnation does
    // not re-stage it after the delivered-positions dedup state has been cleared. Flushless on purpose: dying with
    // an unflushed prune replays the message together with the equally unflushed delivery - at-least-once, exactly
    // like a store-resident message.
    private void PruneDeliveredMessage(EffectId? childId, long? position)
    {
        // Running-flow carrier: the message was captured as its own child effect - delete just that child. A push
        // dropped on its idempotency key never reached staging, so it has no child to delete.
        if (childId is not null)
            _effect.FlushlessClear(childId);

        // Completed-flow carrier: the message came from the inline blob - rewrite the blob without it. Every blob
        // entry addresses a store row, so a row-less message is never one of them.
        if (position is not { } inlinedPosition)
            return;

        var index = _pendingInlinedMessages.FindIndex(m => m.Position == inlinedPosition);
        if (index == -1)
            return;

        _pendingInlinedMessages.RemoveAt(index);
        if (_pendingInlinedMessages.Count == 0)
            _effect.FlushlessClear(PendingMessages.EffectId);
        else
            _effect.FlushlessSet(
                StoredEffect.CreateCompleted(
                    PendingMessages.EffectId,
                    PendingMessages.Encode(_pendingInlinedMessages),
                    alias: null
                )
            );
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
        byte[] MessageTypeBytes,
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

    // The id FlushlessCreateNextChild would append at, without writing - the message is instead written together
    // with the idempotency entry that admitted it, in a single upsert.
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
