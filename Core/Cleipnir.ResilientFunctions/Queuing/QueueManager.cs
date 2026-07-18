using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
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
    // Parent of the per-message received-message children (see _pendingMessageChildren). A dedicated id - not
    // PendingMessages.EffectId - because FlushlessClear cascades to children, and the completed-flow blob lives
    // at (and is cleared via) PendingMessages.EffectId; keeping the carriers on separate ids stops the blob's
    // clear from deleting these children.
    internal static readonly EffectId ReceivedMessagesRoot  = new([ReservedIdPrefix, 2]);

    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly ISerializer _serializer;
    private readonly Effect _effect;
    private readonly FlowExecutionState _flowExecutionState;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly FlowTimeouts _timeouts;
    private readonly UtcNow _utcNow;
    private readonly SettingsWithDefaults _settings;
    private readonly IMessageClearer _messageClearer;
    private readonly IdempotencyKeys _idempotencyKeys;

    private readonly SemaphoreSlim _initializeSemaphore = new(1);
    private readonly SemaphoreSlim _fetchSemaphore = new(1);
    private readonly Lock _lock = new();
    private readonly List<MessageData> _toDeliver = new();
    private readonly List<Subscription> _subscriptions = new();
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();
    // Messages inlined into the pending-messages effect while the flow was completed - staged at initialization
    // and pruned from the durable entry as they are delivered (see PruneDeliveredPendingMessage).
    private readonly Dictionary<long, StoredMessage> _pendingInlinedMessages = new();
    // Received messages captured as individual child effects under ReceivedMessagesRoot (position -> child id).
    // A child is created the moment a message is staged in ProcessMessages and deleted again when the message is
    // delivered or deduped - the running-flow analogue of _pendingInlinedMessages' completed-flow blob.
    private readonly Dictionary<long, EffectId> _pendingMessageChildren = new();
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
        SettingsWithDefaults settings,
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
        _settings = settings;
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
                positions.Clear();
                _effect.FlushlessUpsert(DeliveredPositionsId, positions, alias: null);
            }

            // Re-stage the received-message children a prior incarnation left behind: each message it had staged
            // but not yet delivered persists as its own child effect. A child whose position was already
            // delivered (replayed above) is pruned rather than re-delivered - the analogue of the delivered-
            // positions store-row clear above.
            var stagedChildren = new List<StoredMessage>();
            foreach (var childId in _effect.GetChildren(ReceivedMessagesRoot))
            {
                var message = PendingMessages.DecodeMessage(_effect.Get<byte[]>(childId));

                // A row-less child (control-panel authored) has no store position - assign a synthetic negative
                // one derived from the child index. It is unique (child indexes are), sorts row-less messages in
                // child order before any store row, and every store-facing use of it (row delete, ignore-set) is
                // a harmless no-op since no row can ever carry a negative position.
                if (!message.RowBacked)
                    message = message with { Position = long.MinValue + childId.Id };

                bool alreadyDelivered;
                lock (_lock)
                    alreadyDelivered = _fetchedPositions.Contains(message.Position);
                if (alreadyDelivered)
                {
                    _effect.FlushlessClear(childId);
                    continue;
                }

                // Pre-register the child so ProcessMessages re-stages it without creating a second child.
                lock (_lock)
                    _pendingMessageChildren[message.Position] = childId;
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
                    foreach (var pendingMessage in pendingMessages)
                        _pendingInlinedMessages[pendingMessage.Position] = pendingMessage;

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
    /// A push that hits a disposed (dying) instance reopens its positions instead of dropping them: the
    /// MessageWatchdog already marked them as pushed, so a silent drop would strand the messages in the
    /// ignore-set and lose the flow's wake-up.
    /// </summary>
    public async Task Push(IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
            return;

        if (_disposed)
        {
            _messageClearer.ReopenPositions(messages.Select(m => m.Position));
            return;
        }

        if (!_initialized)
        {
            try
            {
                await Initialize();
            }
            catch (ObjectDisposedException)
            {
                _messageClearer.ReopenPositions(messages.Select(m => m.Position));
                return;
            }
        }

        await _fetchSemaphore.WaitAsync();
        try
        {
            if (_thrownException == null)
                ProcessMessages(messages);

            // A poisoned queue manager (message deserialization failed) cannot deliver. Reopen the batch's
            // unstaged positions so the messages are refetched and handed to a restarted incarnation, whose
            // subscription then surfaces the failure and fails the flow - otherwise a concurrently suspending
            // flow would never learn of the poisoned message and both would be stranded.
            if (_thrownException != null)
            {
                List<long> unstagedPositions;
                lock (_lock)
                    unstagedPositions = messages
                        .Select(m => m.Position)
                        .Where(position => !_fetchedPositions.Contains(position))
                        .ToList();
                _messageClearer.ReopenPositions(unstagedPositions);
            }
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
        Func<MessageData?, IEnumerable<EffectResult>> captureMessage)
    {
        if (_thrownException is { } pre)
            throw pre;

        var subscription = new Subscription(messageId, predicate, captureMessage);
        lock (_lock)
            _subscriptions.Add(subscription);

        DeliverMessages();
        if (subscription.Tcs.Task.IsCompleted)
            return (await subscription.Tcs.Task)?.Envelope;

        if (timeout != null)
            _timeouts.AddTimeout(messageId, timeout.Value);

        var now = _utcNow();
        var maxWaitAt = now + _settings.MessagesDefaultMaxWaitForCompletion;
        var delayCts = new CancellationTokenSource();
        var fireAt = timeout.HasValue && timeout.Value < maxWaitAt ? timeout.Value : maxWaitAt;
        _ = Task.Delay((fireAt - now).RoundUpToZero(), delayCts.Token)
            .ContinueWith(_ => ExpireSubscription(subscription, timeout, messageId), TaskContinuationOptions.OnlyOnRanToCompletion);

        _flowExecutionState.SubflowWaiting();
        try
        {
            var msgData = await subscription.Tcs.Task;
            return msgData?.Envelope;
        }
        finally
        {
            await _flowExecutionState.ResumeSubflow();

            await delayCts.CancelAsync();
            delayCts.Dispose();
        }
    }

    // Caller must hold _fetchSemaphore. Deserializes, dedups by idempotency-key and by already-fetched
    // position (so pushes are idempotent), and stages messages for delivery.
    private void ProcessMessages(IReadOnlyList<StoredMessage> messages)
    {
        foreach (var message in messages)
        {
            var (messageContent, messageType, position, _, idempotencyKey, sender, receiver) = message;

            bool alreadyFetched;
            lock (_lock)
                alreadyFetched = _fetchedPositions.Contains(position);
            if (alreadyFetched)
                continue;

            try
            {
                var msg = _serializer.Deserialize(messageContent, _serializer.ResolveType(messageType)!);

                if (idempotencyKey != null && !_idempotencyKeys.Add(idempotencyKey, position))
                {
                    lock (_lock)
                    {
                        // Synthetic (negative) positions stay out of the durable delivered-positions list: they
                        // have no row to clear, and child indexes may be reused after pruning - a persisted
                        // synthetic would falsely dedup a later message that lands on the same index. The child
                        // prune below is their durable record instead.
                        if (position >= 0)
                        {
                            _deliveredPositions.Add(position);
                            _effect.FlushlessUpsert(DeliveredPositionsId, _deliveredPositions.ToList(), alias: null);
                        }
                        PruneDeliveredPendingMessage(position);
                    }
                    continue;
                }

                var envelope = new Envelope(msg, receiver, sender);
                var messageData = new MessageData(
                    envelope,
                    position,
                    messageContent,
                    messageType,
                    receiver,
                    sender
                );
                lock (_lock)
                {
                    // Keep the staged messages position-sorted so delivery order stays append order even when two
                    // pushers (the MessageWatchdog poll and an initialization-time push) stage batches out of order.
                    var insertAt = _toDeliver.FindIndex(staged => staged.Position > position);
                    if (insertAt == -1)
                        _toDeliver.Add(messageData);
                    else
                        _toDeliver.Insert(insertAt, messageData);
                    _fetchedPositions.Add(position);

                    // Durably capture the received message as its own child effect the moment it is staged; it is
                    // deleted again when the message is delivered or idempotency-deduped (PruneDeliveredPendingMessage).
                    // Flushless, so it costs no I/O and dies with an equally-unflushed delivery - recovery then stays
                    // store-backed and at-least-once. Skipped when the position already has a child (re-staged from an
                    // existing child at initialization).
                    if (!_pendingMessageChildren.ContainsKey(position))
                        _pendingMessageChildren[position] =
                            _effect.FlushlessCreateNextChild(ReceivedMessagesRoot, PendingMessages.EncodeMessage(message));
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

    private void ExpireSubscription(Subscription subscription, DateTime? timeout, EffectId id)
    {
        lock (_lock)
            if (!_subscriptions.Remove(subscription)) //has the subscription been resolved
                return;

        if (timeout.HasValue && _utcNow() >= timeout.Value)
        {
            _effect.FlushlessUpserts(subscription.CaptureMessage(null));
            _timeouts.RemoveTimeout(id);
            subscription.Tcs.TrySetResult(null);
        }
        else
        {
            subscription.Tcs.TrySetException(new SuspendInvocationException());
        }
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
            deliveredPositions.Clear();
            _effect.FlushlessUpsert(DeliveredPositionsId, deliveredPositions, alias: null);
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
            for (var subscriptionIndex = 0; subscriptionIndex < _subscriptions.Count; subscriptionIndex++)
            {
                var subscription = _subscriptions[subscriptionIndex];
                for (var matchIndex = 0; matchIndex < _toDeliver.Count; matchIndex++)
                    if (subscription.Predicate(_toDeliver[matchIndex].Envelope))
                    {
                        var msg = _toDeliver[matchIndex];
                        _toDeliver.RemoveAt(matchIndex);
                        // Synthetic (negative) positions stay out of the durable delivered-positions list - see
                        // the equivalent guard in ProcessMessages; the child prune below is their durable record.
                        if (msg.Position >= 0)
                            _deliveredPositions.Add(msg.Position);
                        _subscriptions.RemoveAt(subscriptionIndex);

                        _effect.FlushlessUpserts(
                            subscription.CaptureMessage(msg)
                                .Append(EffectResult.Create(DeliveredPositionsId, _deliveredPositions.ToList()))
                        );
                        // Same pending-change batch as the capture above - the prune, the captured message and
                        // the delivered position land in one atomic effect write at the next flush.
                        PruneDeliveredPendingMessage(msg.Position);

                        _timeouts.RemoveTimeout(subscription.EffectId);
                        subscription.Tcs.TrySetResult(msg);
                        DeliverMessages();
                        return;
                    }
            }
    }

    // Caller must hold _lock. Removes a delivered (or idempotency-deduped) message from its durable carrier - the
    // per-message child effect (running flow) and/or the completed-flow inline blob - so a later incarnation does
    // not re-stage it after the delivered-positions dedup state has been cleared. Flushless on purpose: dying with
    // an unflushed prune replays the message together with the equally unflushed delivery - at-least-once, exactly
    // like a store-resident message.
    private void PruneDeliveredPendingMessage(long position)
    {
        // Running-flow carrier: the message was captured as its own child effect - delete just that child.
        if (_pendingMessageChildren.Remove(position, out var childId))
            _effect.FlushlessClear(childId);

        // Completed-flow carrier: the message came from the inline blob - rewrite the blob without it.
        if (!_pendingInlinedMessages.Remove(position))
            return;

        if (_pendingInlinedMessages.Count == 0)
            _effect.FlushlessClear(PendingMessages.EffectId);
        else
            _effect.FlushlessSet(
                StoredEffect.CreateCompleted(
                    PendingMessages.EffectId,
                    PendingMessages.Encode(_pendingInlinedMessages.Values.OrderBy(m => m.Position).ToList()),
                    alias: null
                )
            );
    }

    private void FailAllSubscriptions(Exception exception)
    {
        lock (_lock)
        {
            foreach (var sub in _subscriptions)
                sub.Tcs.TrySetException(exception);
            _subscriptions.Clear();
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
                _messageClearer.ReopenPositions(_toDeliver.Select(m => m.Position));
                foreach (var messageData in _toDeliver)
                    _fetchedPositions.Remove(messageData.Position);
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

    public record MessageData(
        Envelope Envelope,
        long Position,
        byte[] MessageContentBytes,
        byte[] MessageTypeBytes,
        string? Receiver,
        string? Sender
    );

    private record Subscription(EffectId EffectId, MessagePredicate Predicate, Func<MessageData?, IEnumerable<EffectResult>> CaptureMessage)
    {
        public TaskCompletionSource<MessageData?> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
