using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

public delegate bool MessagePredicate(Envelope envelope);

public class QueueManager(
    FlowId flowId, 
    StoredId storedId, 
    IMessageStore messageStore, 
    ISerializer serializer, 
    Effect effect, 
    UnhandledExceptionHandler unhandledExceptionHandler,
    FlowTimeouts timeouts,
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
    private readonly List<EnvelopeWithPosition> _toDeliver = new();
    private readonly List<long> _skipPositions = new();
    private readonly HashSet<long> _deliveredPositions = new();

    private IdempotencyKeys? _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private readonly SemaphoreSlim _deliverySemaphore = new(1, 1);

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
        }
        finally
        {
            _semaphoreSlim.Release();
        }
        
        await FetchMessagesOnce();
        _ = Task.Run(FetchMessages);
        _ = Task.Run(CheckTimeouts);
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        await Initialize();
        return new QueueClient(this, serializer, utcNow);
    }

    public async Task FetchMessagesOnce()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(QueueManager)} is disposed already");
        
        await _semaphoreSlim.WaitAsync();
        try
        {
            List<long> skipPositions;
            lock (_lock)
                skipPositions = _toDeliver
                    .Select(m => m.Position)
                    .Concat(_deliveredPositions)
                    .Concat(_skipPositions)
                    .ToList();

            var messages = await messageStore.GetMessages(storedId, skipPositions);

            foreach (var (messageContent, messageType, position, idempotencyKey, sender, receiver) in messages)
            {
                try
                {
                    var msg = serializer.Deserialize(messageContent, serializer.ResolveType(messageType)!);

                    // NoOp messages are immediately deleted and not delivered
                    if (msg is NoOp)
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

                    var envelope = new Envelope(msg, receiver, sender);
                    var envWithPosition = new EnvelopeWithPosition(envelope, position, idempotencyKeyResult);
                    lock (_lock)
                        _toDeliver.Add(envWithPosition);
                }
                catch (Exception e)
                {
                    unhandledExceptionHandler.Invoke(flowId.Type, e);
                    lock (_lock)
                        _skipPositions.Add(position);
                }
            }

            if (messages.Any())
                _ = TryToDeliverAsync();
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async Task FetchMessages()
    {
        while (!_disposed)
        {
            await FetchMessagesOnce();
            await Task.Delay(100);
        }
    }

    public async Task AfterFlush()
    {
        await _semaphoreSlim.WaitAsync();
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
        }
        catch (Exception exception)
        {
            unhandledExceptionHandler.Invoke(flowId.Type, exception);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private async Task TryToDeliverAsync()
    {
        await _deliverySemaphore.WaitAsync();
        try
        {
            while (true)
            {
                List<EnvelopeWithPosition> messagesToDeliver;
                List<KeyValuePair<EffectId, Subscription>> subscribers;
                lock (_lock)
                {
                    messagesToDeliver = _toDeliver.ToList();
                    subscribers = _subscribers.ToList();
                }

                var delivered = false;
                foreach (var messageWithPosition in messagesToDeliver)
                {
                    if (delivered) break;

                    foreach (var idAndSubscription in subscribers)
                    {
                        var (effectId, subscription) = idAndSubscription;
                        if (subscription.Predicate(messageWithPosition.Envelope))
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

                            await effect.Upsert(_toRemoveNextIndex, toRemoveIndex, alias: null, flush: false);

                            var toRemoveId = new EffectId([-1, 0, toRemoveIndex]);
                            var envelopeAndEffectResults = new EnvelopeAndEffectResults(
                                messageWithPosition.Envelope,
                                messageWithPosition.IdempotencyKeyResult == null
                                ? [new EffectResult(toRemoveId, messageWithPosition.Position, Alias: null)]
                                : [new EffectResult(toRemoveId, messageWithPosition.Position, Alias: null), messageWithPosition.IdempotencyKeyResult]
                            );
                            subscription.Tcs.SetResult(envelopeAndEffectResults);

                            delivered = true;
                            break;
                        }
                    }
                }

                if (!delivered)
                    break;
            }
        }
        catch (Exception e)
        {
            unhandledExceptionHandler.Invoke(flowId.Type, e);
        }
        finally
        {
            _deliverySemaphore.Release();
        }
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

            await Task.Delay(100);
        }
    }
    
    public async Task<EnvelopeAndEffectResults?> Subscribe(EffectId effectId, MessagePredicate predicate, DateTime? timeout, EffectId timeoutId, TimeSpan? maxWait)
    {
        var tcs = new TaskCompletionSource<EnvelopeAndEffectResults?>();
        lock (_lock)
            _subscribers[effectId] = new Subscription(predicate, tcs, timeout, timeoutId);

        if (timeout != null)
            timeouts.AddTimeout(timeoutId!, timeout.Value);

        _ = TryToDeliverAsync();

        await Task.WhenAny(tcs.Task, Task.Delay(maxWait ?? settings.MessagesDefaultMaxWaitForCompletion));

        if (!tcs.Task.IsCompleted)
            throw new SuspendInvocationException();

        timeouts.RemoveTimeout(timeoutId);
        return await tcs.Task;
    }

    public record EnvelopeWithPosition(Envelope Envelope, long Position, EffectResult? IdempotencyKeyResult);
    public record EnvelopeAndEffectResults(Envelope Message, IEnumerable<EffectResult> EffectResults);
    private record Subscription(MessagePredicate Predicate, TaskCompletionSource<EnvelopeAndEffectResults?> Tcs, DateTime? Timeout, EffectId? TimeoutId);

    public void Dispose() => _disposed = true;
}