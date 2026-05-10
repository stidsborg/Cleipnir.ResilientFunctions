using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

internal class FetchedMessages
{
    private const int ReservedIdPrefix = -1;
    private static readonly EffectId PendingDeletionsRoot = new([ReservedIdPrefix, 0]);
    private static EffectId          PendingDeletion(int index) => new([ReservedIdPrefix, 0, index]);
    private static readonly EffectId IdempotencyKeysRoot   = new([ReservedIdPrefix, -1]);

    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    private readonly Effect _effect;
    private readonly FlowState _flowState;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly FlowTimeouts _timeouts;
    private readonly SettingsWithDefaults _settings;
    private readonly IdempotencyKeys _idempotencyKeys;

    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly Lock _lock = new();
    private readonly List<MessageData> _toDeliver = new();
    private readonly HashSet<long> _fetchedPositions = new();
    private readonly List<Subscription> _subscriptions = new();
    private int _nextToRemoveIndex;
    private volatile Exception? _thrownException;

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
        _settings = settings;
        _idempotencyKeys = new IdempotencyKeys(IdempotencyKeysRoot, _effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);
    }

    public async Task Initialize()
    {
        _idempotencyKeys.Initialize();

        _nextToRemoveIndex = await _effect.CreateOrGet(PendingDeletionsRoot, 0, alias: null, flush: false);
        var children = _effect.GetChildren(PendingDeletionsRoot);
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
            TryDispatch();
            _semaphore.Release();
        }
    }

    public async Task<MatchResult?> WaitForMessageOrTimeout(EffectId timeoutId, MessagePredicate predicate, DateTime? timeout)
    {
        var subscription = new Subscription(predicate, timeout);

        lock (_lock)
            _subscriptions.Add(subscription);

        TryDispatch();

        var waitTask = timeout != null
            ? _timeouts.AddTimeout(timeoutId, timeout.Value)
            : Task.Delay(_settings.MessagesDefaultMaxWaitForCompletion);

        _flowState.SubflowWaiting();
        await Task.WhenAny(subscription.Tcs.Task, waitTask);
        var success = _flowState.ResumeSubflow();
        if (!success)
            await new TaskCompletionSource().Task;

        lock (_lock)
        {
            var stillRegistered = _subscriptions.Remove(subscription);
            if (stillRegistered)
                subscription.Tcs.TrySetResult(null);
        }

        var result = await subscription.Tcs.Task;
        if (result != null)
            _timeouts.RemoveTimeout(timeoutId);

        return result;
    }

    public async Task AfterFlush()
    {
        await _semaphore.WaitAsync();
        try
        {
            var children = _effect.GetChildren(PendingDeletionsRoot);
            var nonDirtyChildren = new List<EffectId>();
            foreach (var childId in children)
                if (!_effect.IsDirty(childId))
                    nonDirtyChildren.Add(childId);

            if (nonDirtyChildren.Any())
            {
                var positions = new List<long>();
                foreach (var nonDirtyChild in nonDirtyChildren)
                {
                    var position = _effect.Get<long>(nonDirtyChild);
                    positions.Add(position);
                }

                await _messageStore.DeleteMessages(_storedId, positions);
                foreach (var nonDirtyChild in nonDirtyChildren)
                    await _effect.Clear(nonDirtyChild, flush: false);

                lock (_lock)
                    foreach (var position in positions)
                        _fetchedPositions.Remove(position);
            }
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

    private void TryDispatch()
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
                        var positionToRemoveIndex = _nextToRemoveIndex++;
                        var toRemoveId = PendingDeletion(positionToRemoveIndex);
                        _effect.FlushlessUpsert(PendingDeletionsRoot, _nextToRemoveIndex, alias: null);
                        _subscriptions.RemoveAt(subscriptionIndex);
                        subscription.Tcs.TrySetResult(new MatchResult(msg, toRemoveId));
                        TryDispatch();
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

    public record MessageData(
        Envelope Envelope,
        long Position,
        byte[] MessageContentBytes,
        byte[] MessageTypeBytes,
        string? Receiver,
        string? Sender
    );

    public record MatchResult(MessageData Message, EffectId ToRemoveId);

    private record Subscription(MessagePredicate Predicate, DateTime? Timeout)
    {
        public TaskCompletionSource<MatchResult?> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
