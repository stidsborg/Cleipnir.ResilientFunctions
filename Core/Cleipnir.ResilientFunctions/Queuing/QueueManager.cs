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
    private static readonly EffectId DeliveredPositionsId = new([ReservedIdPrefix, 0]);
    private static readonly EffectId IdempotencyKeysRoot   = new([ReservedIdPrefix, -1]);

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
    private readonly Func<Task>? _pushPendingMessages;
    private readonly IdempotencyKeys _idempotencyKeys;

    private readonly SemaphoreSlim _initializeSemaphore = new(1);
    private readonly SemaphoreSlim _fetchSemaphore = new(1);
    private readonly Lock _lock = new();
    private readonly List<MessageData> _toDeliver = new();
    private readonly List<Subscription> _subscriptions = new();
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();
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
        Func<Task>? pushPendingMessages = null,
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
        _pushPendingMessages = pushPendingMessages;
        _idempotencyKeys = new IdempotencyKeys(IdempotencyKeysRoot, _effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);

        // Attach to the flow state immediately - not first at initialization - so a push arriving before the flow's
        // first message interaction is processed (Push self-initializes) instead of being dropped by the flow state.
        flowExecutionState.QueueManager = this;
    }

    private async Task Initialize(bool pushPendingMessages)
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

            _initialized = true;
            _effect.RegisterQueueManager(this);

            // Kick an immediate fetch-and-push so a freshly started/resumed flow receives its pending messages now
            // rather than waiting for the next MessageWatchdog poll. _initialized is already set, so the resulting
            // push routes straight into ProcessMessages for this flow without re-running Initialize. Skipped when
            // initialization is triggered by an incoming Push: the pusher already holds an in-flight batch, and a
            // nested fetch would stage any newer messages ahead of that older batch, reordering delivery.
            if (pushPendingMessages && _pushPendingMessages != null)
                await _pushPendingMessages();
        }
        finally
        {
            _initializeSemaphore.Release();
        }
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        if (!_initialized)
            await Initialize(pushPendingMessages: true);
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

    internal void Interrupt()
    {
        if (_disposed)
            return;

        DeliverMessages();
    }

    /// <summary>
    /// Pushes messages fetched elsewhere (the MessageWatchdog, or the in-hand messages handed over on restart)
    /// straight into the delivery pipeline, avoiding a per-flow re-fetch. Ensures the queue manager is initialized
    /// first so the idempotency-key state is loaded before the messages are processed. Idempotent: positions
    /// already processed are skipped by ProcessMessages.
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
                await Initialize(pushPendingMessages: false);
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
            if (_thrownException != null)
                return;

            ProcessMessages(messages);
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
        foreach (var (messageContent, messageType, position, _, idempotencyKey, sender, receiver) in messages)
        {
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
                        _deliveredPositions.Add(position);
                        _effect.FlushlessUpsert(DeliveredPositionsId, _deliveredPositions.ToList(), alias: null);
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
                        _deliveredPositions.Add(msg.Position);
                        _subscriptions.RemoveAt(subscriptionIndex);

                        _effect.FlushlessUpserts(
                            subscription.CaptureMessage(msg)
                                .Append(EffectResult.Create(DeliveredPositionsId, _deliveredPositions.ToList()))
                        );

                        _timeouts.RemoveTimeout(subscription.EffectId);
                        subscription.Tcs.TrySetResult(msg);
                        DeliverMessages();
                        return;
                    }
            }
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
            if (_toDeliver.Count == 0)
                return;

            _messageClearer.ReopenPositions(_toDeliver.Select(m => m.Position));
            foreach (var messageData in _toDeliver)
                _fetchedPositions.Remove(messageData.Position);
            _toDeliver.Clear();
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
