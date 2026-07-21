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

internal class QueueManager : IDisposable
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

    private readonly SemaphoreSlim _initializeSemaphore = new(1);
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
    private volatile Exception? _thrownException;
    private bool _initialized;
    private volatile bool _disposed;

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
        int maxIdempotencyKeyCount = 100,
        TimeSpan? maxIdempotencyKeyTtl = null)
    {
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

        // Attach to the flow state immediately - not first at initialization - so a push arriving before the flow's
        // first message interaction is processed (Push self-initializes) instead of being dropped by the flow state.
        flowExecutionState.QueueManager = this;
    }

    private async Task Initialize()
    {
        await _initializeSemaphore.WaitAsync();
        try
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(QueueManager)} has already been disposed");
            if (_initialized)
                return;

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
            // positions store-row clear above.
            var stagedChildren = new List<IncomingMessage>();
            foreach (var childId in _effect.GetChildren(StagedMessagesRoot))
            {
                // The child travels with the message: it is the identity of a row-less (control-panel authored)
                // message, which has no store position at all, and it stops ProcessMessages from creating a
                // second child for a message that already has one.
                var message = PendingMessages.DecodeMessage(_effect.Get<byte[]>(childId)) with { ChildId = childId };

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

                stagedChildren.Add(message);
            }
            if (stagedChildren.Count > 0)
                ProcessMessages(stagedChildren);

            // Stage messages that were inlined into the effect state while the flow was completed (their store
            // rows are deleted, so this entry is their only carrier). ProcessMessages dedups them against the
            // replayed delivered positions and the persisted idempotency keys; running it here without the fetch
            // semaphore is safe - pushes acquire the semaphore only after Initialize has completed.
            var pendingEntry = _effect.GetStoredEffect(PendingMessages.EffectId);
            if (pendingEntry?.Result is { Length: > 0 } pendingBytes)
            {
                var pendingMessages = PendingMessages.Decode(pendingBytes);
                lock (_lock)
                    _pendingInlinedMessages.AddRange(pendingMessages);

                ProcessMessages(pendingMessages);
            }

            _initialized = true;
            _effect.RegisterQueueManager(this);
        }
        finally
        {
            _initializeSemaphore.Release();
        }
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        if (!_initialized)
            await Initialize();
        return new QueueClient(this, _serializer, _utcNow);
    }

    // Re-evaluates the already-pushed messages against the current subscriptions. The queue manager no longer reads
    // from the message store: messages arrive exclusively via Push (the MessageWatchdog poll and the restart
    // hand-over), so this only flushes whatever has already been staged for delivery.
    public Task FetchMessagesOnce()
    {
        if (!_disposed)
            DeliverMessages();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Pushes messages fetched elsewhere (the MessageWatchdog, or the in-hand messages handed over on restart)
    /// straight into the delivery pipeline, avoiding a per-flow re-fetch. Ensures the queue manager is initialized
    /// first so the idempotency-key state is loaded before the messages are processed. Idempotent: positions
    /// already processed are skipped by ProcessMessages. Both routes strip empty (restart-poke) messages before
    /// handing over, so every message arriving here carries a deliverable payload.
    ///
    /// Returns true once the batch has been taken over (staged, delivered or deduped): the queue manager now owns
    /// the messages, and its own dispose path reopens anything left undelivered. Returns false when the batch could
    /// not be handled - the instance was disposed (the flow is finishing) or poisoned (a message failed to
    /// deserialize) - so the caller reopens the batch's positions and lets the MessageWatchdog re-fetch and
    /// re-deliver them (the MessageWatchdog already marked them pushed, so a silent drop would strand them in the
    /// ignore-set and lose the flow's wake-up). The caller reopens the whole batch: reopen is idempotent, so
    /// re-reopening a message this push already staged (its durable child effect survives) is harmless, and the
    /// next incarnation dedups the re-fetched row against the re-staged child.
    /// </summary>
    public async Task<bool> Push(IReadOnlyList<PushedMessage> messages)
    {
        if (messages.Count == 0)
            return true;

        if (_disposed)
            return false;

        if (!_initialized)
        {
            try
            {
                await Initialize();
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }

        await _fetchSemaphore.WaitAsync();
        try
        {
            if (_thrownException == null)
                ProcessMessages(messages.Select(IncomingMessage.From).ToList());

            // Poisoned (a message failed to deserialize) or disposed while this push was in flight: the batch was
            // not taken over here. Report it so the caller reopens the whole batch - the messages are then
            // refetched and handed to a restarted incarnation, whose subscription surfaces a poisoned message and
            // fails the flow (otherwise a concurrently suspending flow would never learn of it and both would be
            // stranded).
            return _thrownException == null && !_disposed;
        }
        finally
        {
            DeliverMessages();
            // The instance may have been disposed while this push was in flight - reopen whatever it staged so the
            // messages are not stranded in a dead queue manager.
            if (_disposed)
                ReopenUndeliveredStagedMessages();
            _fetchSemaphore.Release();
        }
    }

    public async Task<Envelope?> Subscribe(
        MessagePredicate predicate,
        DateTime? timeout,
        EffectId messageId,
        Func<StagedMessage?, IEnumerable<EffectResult>> captureMessage)
    {
        if (_thrownException is { } pre)
            throw pre;

        var subscription = new Subscription(messageId, predicate, captureMessage);
        lock (_lock)
            _subscriptions.Add(subscription);

        DeliverMessages();
        if (subscription.Tcs.Task.IsCompleted)
        {
            _flowExecutionState.WakeupConsumed();
            return (await subscription.Tcs.Task)?.Envelope;
        }

        if (timeout != null && _utcNow() >= timeout.Value)
        {
            // The user-timeout has already expired (e.g. replay after a suspension) - resolve with no message
            // immediately instead of waiting.
            bool removed;
            lock (_lock)
                removed = _subscriptions.Remove(subscription);
            if (!removed) //a delivery won the race
            {
                _flowExecutionState.WakeupConsumed();
                return (await subscription.Tcs.Task)?.Envelope;
            }

            _effect.FlushlessUpserts(subscription.CaptureMessage(null));
            return null;
        }

        var delayCts = new CancellationTokenSource();
        if (timeout != null)
        {
            _timeouts.AddTimeout(messageId, timeout.Value);
            ArmSubscriptionTimeout(subscription, timeout.Value, delayCts.Token);
        }

        _flowExecutionState.SubflowWaiting();
        try
        {
            // Completes only via a committed TryResolve resolution (delivery, expiry or failure) - a flow that
            // decided to suspend first leaves the task unresolved and this thread parked forever.
            var msgData = await subscription.Tcs.Task;
            return msgData?.Envelope;
        }
        finally
        {
            _flowExecutionState.ResumeResolvedSubflow();

            // Only removed after passing the resume gate: an unresolved waiter never removes its timeout, so a
            // suspension that overtook the wake-up still finds it registered and postpones to it instead of
            // suspending without a wake-up trigger.
            if (timeout != null)
                _timeouts.RemoveTimeout(messageId);

            await delayCts.CancelAsync();
            delayCts.Dispose();
        }
    }

    // Caller must hold _fetchSemaphore. Deserializes, dedups by idempotency-key and by already-fetched
    // position (so pushes are idempotent), and stages messages for delivery.
    private void ProcessMessages(IReadOnlyList<IncomingMessage> messages)
    {
        foreach (var message in messages)
        {
            var (messageContent, messageType, position, idempotencyKey, sender, receiver) = message;

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

            try
            {
                var msg = _serializer.Deserialize(messageContent, _serializer.ResolveType(messageType)!);

                // A message re-staged from its own child is not re-checked: its key was written in the same upsert
                // as the child itself, so a child that survived to be re-read has its key recorded alongside it.
                // Re-checking would only dedup the message against its own entry.
                var claimsIdempotencyKey = idempotencyKey != null && message.ChildId is null;
                var idempotencyEntry = claimsIdempotencyKey ? _idempotencyKeys.Reserve(idempotencyKey!) : null;

                if (claimsIdempotencyKey && idempotencyEntry is null)
                {
                    lock (_lock)
                    {
                        // A disposed instance must not record a drop: disposal has already reopened and cleared the
                        // delivered-positions set and its flush runs AFTER disposal, so recording now would overwrite
                        // the pending list with a nearly-empty one, durably erasing the incarnation's delivered
                        // markings while their reopened rows live on to be redelivered. Skip instead - Push reports
                        // the batch unhandled and the caller reopens the whole batch, so the next incarnation
                        // re-fetches and re-dedups the message.
                        if (_disposed)
                            continue;

                        RecordDeliveredStoreRow(position);
                        PruneDeliveredMessage(message.ChildId, position);
                    }
                    continue;
                }

                var envelope = new Envelope(msg, receiver, sender);
                lock (_lock)
                {
                    // See the disposed-guard in the idempotency-drop path above: a disposed instance must not stage
                    // or record anything - its state has been reopened and its flush is imminent. Skip and let the
                    // caller's whole-batch reopen recover the message.
                    if (_disposed)
                        continue;

                    // Durably capture the message as its own child effect the moment it is staged; it is
                    // deleted again when the message is delivered or idempotency-deduped (PruneDeliveredMessage).
                    // Flushless, so it costs no I/O and dies with an equally-unflushed delivery - recovery then stays
                    // store-backed and at-least-once. A message re-staged from an existing child reuses it rather
                    // than creating a second one.
                    var childId = message.ChildId;
                    if (childId is null)
                    {
                        childId = NextStagedMessageChildId();

                        // One upsert for the message and the key that admitted it: neither can become durable
                        // without the other, so a recorded key always has its message behind it.
                        _effect.FlushlessUpserts(
                            idempotencyEntry is null
                                ? [EffectResult.Create(childId, PendingMessages.EncodeMessage(message))]
                                : [EffectResult.Create(childId, PendingMessages.EncodeMessage(message)), idempotencyEntry]
                        );
                    }

                    var stagedMessage = new StagedMessage(
                        envelope,
                        position,
                        childId,
                        messageContent,
                        messageType,
                        receiver,
                        sender
                    );

                    // Keep the staged messages in delivery order even when two pushers (the MessageWatchdog poll
                    // and an initialization-time push) stage batches out of order.
                    var insertAt = _toDeliver.FindIndex(staged => CompareDeliveryOrder(staged, stagedMessage) > 0);
                    if (insertAt == -1)
                        _toDeliver.Add(stagedMessage);
                    else
                        _toDeliver.Insert(insertAt, stagedMessage);

                    if (position is { } fetchedPosition)
                        _fetchedPositions.Add(fetchedPosition);
                }
            }
            catch (Exception e)
            {
                _unhandledExceptionHandler.Invoke(_flowId.Type, e);
                _thrownException = e;
                FailAllSubscriptions(e);
                return;
            }
        }
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
            subscription.Tcs.TrySetResult(null);
        });
    }

    public async Task AfterFlush()
    {
        await _fetchSemaphore.WaitAsync();
        try
        {
            if (!_effect.TryGet<List<long>>(DeliveredPositionsId, out var deliveredPositions) || deliveredPositions is null)
                return;

            if (deliveredPositions.Count == 0 || _effect.IsDirty(DeliveredPositionsId))
                return;

            await _messageClearer.Clear(deliveredPositions);
            _effect.FlushlessUpsert(DeliveredPositionsId, NoDeliveredPositions, alias: null);
        }
        catch (Exception exception)
        {
            _unhandledExceptionHandler.Invoke(_flowId.Type, exception);
        }
        finally
        {
            _fetchSemaphore.Release();
        }
    }

    private void DeliverMessages()
    {
        lock (_lock)
        {
            // A disposed instance must not deliver: the capture and delivered-position recording would leak into
            // the dying incarnation's imminent flush while the flow itself never processes the message.
            if (_disposed)
                return;

            for (var subscriptionIndex = 0; subscriptionIndex < _subscriptions.Count; subscriptionIndex++)
            {
                var subscription = _subscriptions[subscriptionIndex];
                for (var matchIndex = 0; matchIndex < _toDeliver.Count; matchIndex++)
                    if (subscription.Predicate(_toDeliver[matchIndex].Envelope))
                    {
                        var index = matchIndex;
                        // Sealed against the suspension decision: a suspended flow must not consume the message -
                        // it stays staged and is reopened at dispose so a restarted incarnation delivers it.
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

    private void FailAllSubscriptions(Exception exception)
    {
        lock (_lock)
        {
            // Sealed against the suspension decision like every other resolution: a flow that has decided to
            // suspend keeps its waiters parked and learns of the poisoned message via the reopened positions
            // instead. Rejected subscriptions stay in the list - the incarnation is dying either way.
            foreach (var sub in _subscriptions.ToList())
                if (_flowExecutionState.TryResolve(() => sub.Tcs.TrySetException(exception)))
                    _subscriptions.Remove(sub);
        }
    }

    /// <summary>
    /// Marks the instance dead and reopens the staged-but-undelivered positions: the invocation is finishing
    /// (suspending or completing), so no subscription will ever consume them here. Reopening lets the
    /// MessageWatchdog re-fetch them and deliver via a restart instead of leaving them stranded in the
    /// ignore-set until the (much slower) postponed-watchdog path picks the flow up.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
        ReopenUndeliveredStagedMessages();
    }

    private void ReopenUndeliveredStagedMessages()
    {
        lock (_lock)
        {
            if (_toDeliver.Count > 0)
            {
                // Only store-addressed messages take part: a row-less message has no row to reopen and never
                // entered the fetched set - its child effect is its carrier and survives on its own.
                var stagedPositions = _toDeliver
                    .Where(stagedMessage => stagedMessage.Position is not null)
                    .Select(stagedMessage => stagedMessage.Position!.Value)
                    .ToList();

                _messageClearer.ReopenPositions(stagedPositions);
                foreach (var stagedPosition in stagedPositions)
                    _fetchedPositions.Remove(stagedPosition);
                _toDeliver.Clear();
            }

            // Delivered and idempotency-deduped positions whose store delete has not landed are reopened too:
            // their markings live in flushless effect state that dies with an incarnation ending without a flush,
            // and restarts no longer hand the store's messages over - without the reopen such positions would be
            // stranded in the ignore-set forever. The re-push is idempotent: durably captured positions replay
            // into the next incarnation's fetched-set and are cleared by its initialization, deduped ones are
            // deduped again, and reopening an already-deleted position is a no-op.
            if (_deliveredPositions.Count > 0)
            {
                _messageClearer.ReopenPositions(_deliveredPositions.ToList());
                _deliveredPositions.Clear();
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
    }
}
