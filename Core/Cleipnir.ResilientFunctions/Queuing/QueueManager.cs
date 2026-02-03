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

    private readonly EffectId _toRemoveNextIndex = new([-1, 0]);
    private readonly EffectId _idempotencyKeysId = new([-1, -1]);
    private readonly List<MessageData> _toDeliver = new();
    private readonly HashSet<long> _fetchedPositions = new();

    private IdempotencyKeys? _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private readonly SemaphoreSlim _semaphore = new(1);
    private bool _initialized = false;
    private volatile bool _disposed;
    
    private volatile Exception? _thrownException = null;

    public async Task Initialize()
    {
        await _semaphore.WaitAsync();
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
            _semaphore.Release();
        }
        
        await FetchMessages();
        _ = Task.Run(StartFetchMessagesLoop);
        _ = Task.Run(CheckTimeouts);
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        await Initialize();
        return new QueueClient(this, serializer, utcNow);
    }

    public async Task FetchMessages()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(QueueManager)} is disposed already");
        
        await _semaphore.WaitAsync();
        try
        {
            if (_thrownException != null)
            {
                foreach (var (_, subscription) in _subscribers)
                    _ = Task.Run(() => subscription.Tcs.TrySetException(_thrownException));

                _subscribers.Clear();
                return;
            }
            
            var skipPositions = _fetchedPositions.ToList();

            var messages = await messageStore.GetMessages(storedId, skipPositions);
            foreach (var (messageContent, messageType, position, idempotencyKey, sender, receiver) in messages)
            {
                try
                {
                    var msg = serializer.Deserialize(messageContent, serializer.ResolveType(messageType)!);

                    var idempotencyKeyResult = idempotencyKey == null
                        ? null
                        : _idempotencyKeys!.Add(idempotencyKey, position);

                    if (idempotencyKey != null && idempotencyKeyResult == null)
                    {
                        await messageStore.DeleteMessages(storedId, [position]);
                        continue;
                    }

                    var envelope = new Envelope(msg, receiver, sender);
                    var messageData = new MessageData(
                        envelope,
                        position,
                        idempotencyKeyResult,
                        messageContent,
                        messageType,
                        receiver,
                        sender
                    );
                    _toDeliver.Add(messageData);
                    _fetchedPositions.Add(position);
                }
                catch (Exception e)
                {
                    unhandledExceptionHandler.Invoke(flowId.Type, e);
                    _thrownException = e;

                    foreach (var (_, subscription) in _subscribers)
                        _ = Task.Run(() => subscription.Tcs.TrySetException(_thrownException));
                    _subscribers.Clear();

                    return;
                }
            }

            if (messages.Any())
                _ = Task.Run(DeliverMessages);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task StartFetchMessagesLoop()
    {
        while (!_disposed && _thrownException == null)
        {
            await FetchMessages();
            await Task.Delay(100);
        }
    }

    public async Task AfterFlush()
    {
        await _semaphore.WaitAsync();
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

                foreach (var position in positions)
                    _fetchedPositions.Remove(position);
            }
        }
        catch (Exception exception)
        {
            unhandledExceptionHandler.Invoke(flowId.Type, exception);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task DeliverMessages()
    {
        await _semaphore.WaitAsync();
        try
        {
            while (true)
            {
                var messages = _toDeliver.ToList();
                var subscribers = _subscribers.ToList();

                var delivered = false;
                foreach (var envelopeWithPosition in messages)
                {
                    if (delivered) break;

                    foreach (var idAndSubscription in subscribers)
                    {
                        var (effectId, subscription) = idAndSubscription;
                        if (subscription.Predicate(envelopeWithPosition.Envelope))
                        {
                            if (!_subscribers.ContainsKey(effectId)) //might have been removed by timeout
                                continue;

                            _toDeliver.Remove(envelopeWithPosition);
                            _subscribers.Remove(effectId);
                            var positionToRemoveIndex = _nextToRemoveIndex++;

                            var toRemoveId = new EffectId([-1, 0, positionToRemoveIndex]);
                            await effect.Upserts(
                                new List<EffectResult>(
                                [
                                    new EffectResult(_toRemoveNextIndex, positionToRemoveIndex, Alias: null),
                                    new EffectResult(toRemoveId, envelopeWithPosition.Position, Alias: null),
                                    new EffectResult(subscription.MessageContentId, envelopeWithPosition.MessageContentBytes, Alias: null),
                                    new EffectResult(subscription.MessageTypeId, envelopeWithPosition.MessageTypeBytes, Alias: null),
                                    new EffectResult(subscription.ReceiverId, envelopeWithPosition.Receiver, Alias: null),
                                    new EffectResult(subscription.SenderId, envelopeWithPosition.Sender, Alias: null),
                                ]).Concat(envelopeWithPosition.IdempotencyKeyResult == null
                                    ? []
                                    : [envelopeWithPosition.IdempotencyKeyResult]),
                                flush: false
                            );
                            
                            _ = Task.Run(() => subscription.Tcs.SetResult(envelopeWithPosition.Envelope));

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
            _semaphore.Release();
        }
    }
    
    public async Task CheckTimeouts()
    {
        while (!_disposed && _thrownException == null)
        {
            List<KeyValuePair<EffectId, Subscription>> expiredSubscriptions;

            await _semaphore.WaitAsync();
            try
            {
                var now = utcNow();
                expiredSubscriptions = _subscribers
                    .Where(s => s.Value.Timeout.HasValue && s.Value.Timeout.Value <= now)
                    .ToList();

                foreach (var expired in expiredSubscriptions)
                    _subscribers.Remove(expired.Key);
            }
            finally
            {
                _semaphore.Release();
            }

            foreach (var (_, subscription) in expiredSubscriptions)
                subscription.Tcs.TrySetResult(null);

            await Task.Delay(100);
        }
    }
    
    public async Task<Envelope?> Subscribe(
        EffectId effectId,
        MessagePredicate predicate,
        DateTime? timeout,
        EffectId timeoutId,
        EffectId messageId,
        EffectId messageTypeId,
        EffectId receiverId,
        EffectId senderId,
        TimeSpan? maxWait)
    {
        if (_thrownException != null)
            throw _thrownException;

        var tcs = new TaskCompletionSource<Envelope?>();

        await _semaphore.WaitAsync();
        try
        {
            _subscribers[effectId] = new Subscription(predicate, tcs, timeout, messageId, messageTypeId, receiverId, senderId);
        }
        finally
        {
            _semaphore.Release();
        }

        if (timeout != null)
            timeouts.AddTimeout(timeoutId, timeout.Value);

        _ = DeliverMessages();

        await Task.WhenAny(tcs.Task, Task.Delay(maxWait ?? settings.MessagesDefaultMaxWaitForCompletion));

        if (!tcs.Task.IsCompleted)
            throw new SuspendInvocationException();

        timeouts.RemoveTimeout(timeoutId);
        return await tcs.Task;
    }

    public record MessageData(
        Envelope Envelope,
        long Position,
        EffectResult? IdempotencyKeyResult,
        byte[] MessageContentBytes,
        byte[] MessageTypeBytes,
        string? Receiver,
        string? Sender
    );

    private record Subscription(
        MessagePredicate Predicate, 
        TaskCompletionSource<Envelope?> Tcs, 
        DateTime? Timeout, 
        EffectId MessageContentId, 
        EffectId MessageTypeId,
        EffectId ReceiverId,
        EffectId SenderId);

    public void Dispose() => _disposed = true;
}