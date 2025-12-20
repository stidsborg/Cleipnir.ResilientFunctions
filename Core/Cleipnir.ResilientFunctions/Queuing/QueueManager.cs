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

public delegate bool MessagePredicate(object message);

public class QueueManager(
    FlowId flowId, 
    StoredId storedId, 
    IMessageStore messageStore, 
    ISerializer serializer, 
    Effect effect, 
    UnhandledExceptionHandler unhandledExceptionHandler,
    FlowMinimumTimeout minimumTimeout,
    UtcNow utcNow,
    int maxIdempotencyKeyCount = 100,
    TimeSpan? maxIdempotencyKeyTtl = null)
    : IDisposable
{
    private readonly Dictionary<EffectId, Subscription> _subscribers = new();
    private readonly Lock _lock = new();
    
    private readonly EffectId _parentId = new([-1]);
    private readonly EffectId _toRemoveNextIndex = new([-1, 0]);
    private readonly EffectId _idempotencyKeysId = new([-1, -1]);
    private readonly List<MessageWithPosition> _toDeliver = new();

    private IdempotencyKeys? _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private bool _delivering;

    private volatile bool _disposed;

    public async Task Initialize()
    {
        effect.RegisterQueueManager(this);

        _idempotencyKeys = new IdempotencyKeys(_idempotencyKeysId, effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);
        
        _nextToRemoveIndex = await effect.CreateOrGet(_toRemoveNextIndex, 0, alias: null, flush: false);
        var children = effect.GetChildren(_toRemoveNextIndex);
        var positions = new List<long>();
        foreach (var childId in children)
        {
            var position = effect.Get<long>(childId);
            positions.Add(position);
        }

        if (positions.Any()) {
           await messageStore.DeleteMessages(storedId, positions);
           foreach (var childId in children) 
               await effect.Clear(childId, flush: false);
        }

        _ = Task.Run(FetchMessages);
        _ = Task.Run(CheckTimeouts);
    }
    
    public async Task FetchMessages()
    {
        while (!_disposed)
        {
            List<long> skipPositions;
            lock (_lock)
                skipPositions = _toDeliver.Select(m => m.Position).ToList();
            
            var messages = await messageStore.GetMessages(storedId, skipPositions);
            var toRemove = new List<long>();
            
            foreach (var (messageContent, messageType, position, idempotencyKey) in messages)
            {
                if (idempotencyKey != null && _idempotencyKeys!.Contains(idempotencyKey, position))
                {
                    toRemove.Add(position);
                    continue;
                }
                if (idempotencyKey != null)
                    await _idempotencyKeys!.Add(idempotencyKey, position);
                
                var msg = serializer.DeserializeMessage(messageContent, messageType);
                var msgWithPosition = new MessageWithPosition(msg, position, idempotencyKey);
                lock (_lock)
                    _toDeliver.Add(msgWithPosition);
            }

            if (toRemove.Any())
                await messageStore.DeleteMessages(storedId, toRemove);
            
            if (messages.Any())
                TryToDeliver();

            await Task.Delay(1_000);
        }
    }

    private bool _isFlushing;
    private bool _flushAgain;
    private bool TryBeginFlush()
    {
        lock (_lock)
            if (_isFlushing)
            {
                _flushAgain = true;
                return true;
            }
            else
            {
                _isFlushing = true;
                return false;
            }
    }
    
    public async Task AfterFlush()
    {
        if (TryBeginFlush())
            return;
        
        while (true)
        {
            try
            {
                var children = effect.GetChildren(_toRemoveNextIndex);
                var nonDirtyChildren = new List<EffectId>();
                foreach (var childId in children)
                    if (!effect.IsDirty(childId))
                        nonDirtyChildren.Add(childId);

                if (nonDirtyChildren.Any())
                {
                    var positions = new List<long>();
                    foreach (var nonDirtyChild in nonDirtyChildren)
                    {
                        var position = effect.Get<long>(nonDirtyChild);
                        positions.Add(position);
                    }

                    await messageStore.DeleteMessages(storedId, positions);
                    foreach (var nonDirtyChild in nonDirtyChildren)
                        await effect.Clear(nonDirtyChild, flush: false);
                }

                lock (_lock)
                    if (_flushAgain)
                        _flushAgain = false;
                    else
                    {
                        _isFlushing = false;
                        return;
                    }
            }
            catch (Exception exception)
            {
                unhandledExceptionHandler.Invoke(flowId.Type, exception);
                lock (_lock)
                {
                    _isFlushing = false;
                    _flushAgain = false;
                }
                return;
            }
        }
    }

    private void TryToDeliver()
    {
        lock (_lock)
            if (_delivering)
                return;
            else
                _delivering = true;

        StartAgain:
        try
        {
            foreach (var messageWithPosition in _toDeliver.ToList())
            foreach (var idAndSubscription in _subscribers.ToList())
            {
                var (effectId, subscription) = idAndSubscription;
                if (subscription.Predicate(messageWithPosition.Message))
                {
                    int toRemoveIndex;
                    lock (_lock)
                    {
                        if (!_subscribers.ContainsKey(effectId)) //might have been removed by timeout
                            continue;

                        _toDeliver.Remove(messageWithPosition);
                        _subscribers.Remove(effectId);
                        toRemoveIndex = _nextToRemoveIndex++;
                    }

                    effect.Upsert(_toRemoveNextIndex, toRemoveIndex, alias: null, flush: false);

                    var toRemoveId = new EffectId([-1, 0, toRemoveIndex]);
                    var msg = new MessageAndEffectResult(
                        messageWithPosition.Message,
                        new EffectResult(toRemoveId, messageWithPosition.Position, Alias: null)
                    );
                    subscription.Tcs.SetResult(msg);

                    goto StartAgain;
                }
            }
        }
        catch (Exception e)
        {
            unhandledExceptionHandler.Invoke(flowId.Type, e);
        }

        lock (_lock)
            _delivering = false;
    }
    
    public async Task CheckTimeouts()
    {
        while (!_disposed)
        {
            var now = utcNow();
            List<KeyValuePair<EffectId, Subscription>> expiredSubscriptions;

            lock (_lock)
            {
                expiredSubscriptions = _subscribers
                    .Where(s => s.Value.Timeout.HasValue && s.Value.Timeout.Value <= now)
                    .ToList();

                foreach (var expired in expiredSubscriptions)
                    _subscribers.Remove(expired.Key);
            }

            foreach (var (_, subscription) in expiredSubscriptions)
                subscription.Tcs.SetResult(null);

            await Task.Delay(500);
        }
    }
    
    public Task<MessageAndEffectResult?> Subscribe(EffectId effectId, MessagePredicate predicate, DateTime? timeout, EffectId timeoutId)
    {
        var tcs = new TaskCompletionSource<MessageAndEffectResult?>();
        lock (_lock)
            _subscribers[effectId] = new Subscription(predicate, tcs, timeout, timeoutId);

        if (timeout != null)
            minimumTimeout.AddTimeout(timeoutId!, timeout.Value);
        
        TryToDeliver();
        
        return tcs.Task;
    }

    public record MessageWithPosition(object Message, long Position, string? IdempotencyKey);
    public record MessageAndEffectResult(object Message, EffectResult EffectResult);
    private record Subscription(MessagePredicate Predicate, TaskCompletionSource<MessageAndEffectResult?> Tcs, DateTime? Timeout, EffectId? TimeoutId);

    public void Dispose() => _disposed = true;
}