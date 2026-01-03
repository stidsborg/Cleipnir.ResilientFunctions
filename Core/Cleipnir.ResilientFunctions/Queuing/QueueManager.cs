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
    SettingsWithDefaults settings,
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
    private readonly HashSet<long> _deliveredPositions = new();

    private IdempotencyKeys? _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private bool _delivering;

    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1);
    private bool _initialized = false;
    private volatile bool _disposed;

    public async Task Initialize()
    {
        await _semaphoreSlim.WaitAsync();
        try
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(QueueManager)} has already been disposed");
            if (_initialized)
                return;
            
            effect.RegisterQueueManager(this);

            _idempotencyKeys = new IdempotencyKeys(_idempotencyKeysId, effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, utcNow);
            _idempotencyKeys.Initialize();

            _nextToRemoveIndex = await effect.CreateOrGet(_toRemoveNextIndex, 0, alias: null, flush: false);
            var children = effect.GetChildren(_toRemoveNextIndex);
            var positions = new List<long>();
            foreach (var childId in children)
            {
                var position = effect.Get<long>(childId);
                positions.Add(position);
            }

            if (positions.Any())
            {
                await messageStore.DeleteMessages(storedId, positions);
                foreach (var childId in children)
                    await effect.Clear(childId, flush: false);
            }

            _initialized = true;
            _ = Task.Run(FetchMessages);
            _ = Task.Run(CheckTimeouts);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        await  Initialize();
        return new QueueClient(this, utcNow);
    }
    
    public async Task FetchMessages()
    {
        while (!_disposed)
        {
            List<long> skipPositions;
            lock (_lock)
                skipPositions = _toDeliver.Select(m => m.Position).Concat(_deliveredPositions).ToList();

            var messages = await messageStore.GetMessages(storedId, skipPositions);
            
            foreach (var (messageContent, messageType, position, idempotencyKey) in messages)
            {
                if (idempotencyKey != null && _idempotencyKeys!.Contains(idempotencyKey))
                {
                    await messageStore.DeleteMessages(storedId, [position]);
                    continue;
                }

                var idempotencyKeyResult = idempotencyKey == null
                    ? null
                    : _idempotencyKeys!.Add(idempotencyKey, position);

                if (idempotencyKey != null && idempotencyKeyResult == null)
                {
                    await messageStore.DeleteMessages(storedId, [position]);
                    continue;
                }

                var msg = serializer.DeserializeMessage(messageContent, messageType);
                var msgWithPosition = new MessageWithPosition(msg, position, idempotencyKeyResult);
                lock (_lock)
                    _toDeliver.Add(msgWithPosition);
            }
            
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
                    
                    lock (_lock)
                        foreach (var position in positions)
                            _deliveredPositions.Remove(position);
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
        List<MessageWithPosition> messagesToDeliver;
        List<KeyValuePair<EffectId, Subscription>> subscribers;
        lock (_lock)
            if (_delivering)
                return;
            else
                _delivering = true;

        StartAgain:
        lock (_lock)
        {
            messagesToDeliver = _toDeliver.ToList();
            subscribers = _subscribers.ToList();
        }

        try
        {
            foreach (var messageWithPosition in messagesToDeliver)
            foreach (var idAndSubscription in subscribers)
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
                        _deliveredPositions.Add(messageWithPosition.Position);
                        _subscribers.Remove(effectId);
                        toRemoveIndex = _nextToRemoveIndex++;
                    }

                    effect.Upsert(_toRemoveNextIndex, toRemoveIndex, alias: null, flush: false);

                    var toRemoveId = new EffectId([-1, 0, toRemoveIndex]);
                    var msg = new MessageAndEffectResults(
                        messageWithPosition.Message,
                        messageWithPosition.IdempotencyKeyResult == null
                        ? [new EffectResult(toRemoveId, messageWithPosition.Position, Alias: null)]
                        : [new EffectResult(toRemoveId, messageWithPosition.Position, Alias: null), messageWithPosition.IdempotencyKeyResult]
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
    
    public async Task<MessageAndEffectResults?> Subscribe(EffectId effectId, MessagePredicate predicate, DateTime? timeout, EffectId timeoutId, TimeSpan? maxWait)
    {
        var tcs = new TaskCompletionSource<MessageAndEffectResults?>();
        lock (_lock)
            _subscribers[effectId] = new Subscription(predicate, tcs, timeout, timeoutId);

        if (timeout != null)
            minimumTimeout.AddTimeout(timeoutId!, timeout.Value);

        TryToDeliver();

        await Task.WhenAny(tcs.Task, Task.Delay(maxWait ?? settings.MessagesDefaultMaxWaitForCompletion));

        if (!tcs.Task.IsCompleted)
            throw new SuspendInvocationException();

        return await tcs.Task;
    }

    public record MessageWithPosition(object Message, long Position, EffectResult? IdempotencyKeyResult);
    public record MessageAndEffectResults(object Message, IEnumerable<EffectResult> EffectResults);
    private record Subscription(MessagePredicate Predicate, TaskCompletionSource<MessageAndEffectResults?> Tcs, DateTime? Timeout, EffectId? TimeoutId);

    public void Dispose() => _disposed = true;
}