using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

internal class FetchedMessages
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

    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly Lock _lock = new();
    private readonly List<MessageData> _toDeliver = new();
    private readonly List<Subscription> _subscriptions = new();
    private volatile Exception? _thrownException;
    
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();

    public Exception? ThrownException => _thrownException;

    public FetchedMessages(
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
        int maxIdempotencyKeyCount,
        TimeSpan? maxIdempotencyKeyTtl)
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

    public async Task Initialize()
    {
        _idempotencyKeys.Initialize();

        var children = _effect.GetChildren(DeliveredPositionsId);
        var positions = new List<long>();
        foreach (var childId in children)
        {
            var position = _effect.Get<long>(childId);
            positions.Add(position);
        }

        if (positions.Any())
        {
            await _messageStore.DeleteMessages(_storedId, positions);
            foreach (var childId in children)
                await _effect.Clear(childId, flush: false);
        }
    }

    public async Task FetchOnce()
    {
        await _semaphore.WaitAsync();
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
            _semaphore.Release();
        }
    }

    public async Task<MessageData?> AddSubscription(EffectId id, MessagePredicate predicate, DateTime? timeout, Func<MessageData?, IEnumerable<EffectResult>> captureMessage)
    {
        if (timeout != null)
            _timeouts.AddTimeout(id, timeout.Value);
        
        var utcNow = _utcNow();
        var waitBeforeNull = (timeout, _settings.MessagesDefaultMaxWaitForCompletion) switch
        {
            (null, { Ticks: 0 })  => utcNow,
            (null, var w)         => utcNow + w,
            ({ } t, { Ticks: 0 }) => utcNow,
            ({ } t, var w)        => t < utcNow + w ? t : utcNow + w
        };
        
        var subscription = new Subscription(id, predicate, Timeout: waitBeforeNull, UserTimeout: timeout, captureMessage);
        lock (_lock)
            _subscriptions.Add(subscription);

        _flowState.SubflowWaiting();
        var result = await subscription.Tcs.Task;
        var success = _flowState.TryResumeSubflow();
        if (!success)
            await new TaskCompletionSource().Task;

        if (result != null)
            return result;
        if (timeout != null && _utcNow() > timeout.Value)
            return null;

        throw new SuspendInvocationException();
    }
    
    public async Task AfterFlush()
    {
        await _semaphore.WaitAsync();
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
            _semaphore.Release();
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

    public void FireTimeouts()
    {
        var now = _utcNow();
        lock (_lock)
            for (var i = _subscriptions.Count - 1; i >= 0; i--)
            {
                var subscription = _subscriptions[i];
                if (subscription.Timeout is { } timeout && timeout <= now)
                {
                    _subscriptions.RemoveAt(i);

                    if (subscription.UserTimeout is { } userTimeout && userTimeout <= now)
                    {
                        _effect.FlushlessUpserts(subscription.CaptureMessage(null));
                        _timeouts.RemoveTimeout(subscription.EffectId);
                    }

                    subscription.Tcs.TrySetResult(null);
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
    

    private record Subscription(EffectId EffectId, MessagePredicate Predicate, DateTime? Timeout, DateTime? UserTimeout, Func<MessageData?, IEnumerable<EffectResult>> CaptureMessage)
    {
        public TaskCompletionSource<MessageData?> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
