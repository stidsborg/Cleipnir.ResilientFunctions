using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    private readonly Effect _effect;
    private readonly FlowState _flowState;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly FlowTimeouts _timeouts;
    private readonly UtcNow _utcNow;
    private readonly SettingsWithDefaults _settings;
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
        IMessageStore messageStore,
        ISerializer serializer,
        Effect effect,
        FlowState flowState,
        UnhandledExceptionHandler unhandledExceptionHandler,
        FlowTimeouts timeouts,
        UtcNow utcNow,
        SettingsWithDefaults settings,
        int maxIdempotencyKeyCount = 100,
        TimeSpan? maxIdempotencyKeyTtl = null)
    {
        _flowId = flowId;
        _storedId = storedId;
        _messageStore = messageStore;
        _serializer = serializer;
        _effect = effect;
        _flowState = flowState;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _timeouts = timeouts;
        _utcNow = utcNow;
        _settings = settings;
        _idempotencyKeys = new IdempotencyKeys(IdempotencyKeysRoot, _effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);
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
                await _messageStore.DeleteMessages(_storedId, positions);
                positions.Clear();
                _effect.FlushlessUpsert(DeliveredPositionsId, positions, alias: null);
            }

            _initialized = true;
            _effect.RegisterQueueManager(this);
            _flowState.QueueManager = this;
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

    public Task FetchMessagesOnce()
        => _disposed ? Task.CompletedTask : FetchAndNotify();

    internal void Interrupt()
    {
        if (_disposed)
            return;
        _ = FetchMessagesOnce();
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

        await FetchAndNotify();
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

        _flowState.SubflowWaiting();
        try
        {
            var msgData = await subscription.Tcs.Task;
            return msgData?.Envelope;
        }
        finally
        {
            await _flowState.ResumeSubflow();

            await delayCts.CancelAsync();
            delayCts.Dispose();
        }
    }

    private async Task FetchAndNotify()
    {
        await _fetchSemaphore.WaitAsync();
        try
        {
            if (_thrownException != null)
                return;

            List<long> skipPositions;
            lock (_lock)
                skipPositions = _fetchedPositions.ToList();

            var messages = await _messageStore.GetMessages(_storedId, skipPositions);
            foreach (var (messageContent, messageType, position, idempotencyKey, sender, receiver) in messages)
            {
                try
                {
                    var msg = _serializer.Deserialize(messageContent, _serializer.ResolveType(messageType)!);

                    if (idempotencyKey != null && !_idempotencyKeys.Add(idempotencyKey, position))
                    {
                        await _messageStore.DeleteMessages(_storedId, [position]);
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
                        _toDeliver.Add(messageData);
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
        finally
        {
            DeliverMessages();
            _fetchSemaphore.Release();
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

            await _messageStore.DeleteMessages(_storedId, deliveredPositions);
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

    public void Dispose() => _disposed = true;

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
