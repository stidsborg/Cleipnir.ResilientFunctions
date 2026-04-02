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
    FlowsManager flowsManager,
    int maxIdempotencyKeyCount = 100,
    TimeSpan? maxIdempotencyKeyTtl = null)
    : IDisposable
{
    private readonly Lock _lock = new();

    private readonly EffectId _toRemoveNextIndex = new([-1, 0]);
    private readonly EffectId _idempotencyKeysId = new([-1, -1]);
    private readonly List<MessageData> _toDeliver = new();
    private readonly HashSet<long> _fetchedPositions = new();

    private IdempotencyKeys? _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1);
    private bool _initialized = false;
    private volatile bool _disposed;

    private volatile Exception? _thrownException = null;
    private TaskCompletionSource _pulse = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task Initialize()
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
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        if (!_initialized)
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
            if (_thrownException != null)
                return;

            List<long> skipPositions;
            lock (_lock)
                skipPositions = _fetchedPositions.ToList();

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
                    lock (_lock)
                    {
                        _toDeliver.Add(messageData);
                        _fetchedPositions.Add(position);
                    }
                }
                catch (Exception e)
                {
                    unhandledExceptionHandler.Invoke(flowId.Type, e);
                    _thrownException = e;
                    return;
                }
            }
        }
        finally
        {
            PulseAll();
            _semaphoreSlim.Release();
        }
    }

    private (MessageData? Matched, int PositionToRemoveIndex, Task PulseTask) TryTakeMessage(MessagePredicate predicate)
    {
        lock (_lock)
        {
            for (var i = 0; i < _toDeliver.Count; i++)
                if (predicate(_toDeliver[i].Envelope))
                {
                    var matched = _toDeliver[i];
                    _toDeliver.RemoveAt(i);
                    var positionToRemoveIndex = _nextToRemoveIndex++;
                    effect.FlushlessUpsert(_toRemoveNextIndex, _nextToRemoveIndex, alias: null);
                    return (matched, positionToRemoveIndex, _pulse.Task);
                }

            return (null, 0, _pulse.Task);
        }
    }

    public async Task FetchMessages()
    {
        while (!_disposed && _thrownException == null)
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
                        _fetchedPositions.Remove(position);
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

    private void PulseAll()
    {
        TaskCompletionSource oldPulse;
        lock (_lock)
        {
            oldPulse = _pulse;
            _pulse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        oldPulse.TrySetResult();
    }

    public async Task<Envelope?> Subscribe(
        MessagePredicate predicate,
        DateTime? timeout,
        EffectId timeoutId,
        EffectId messageId,
        EffectId messageTypeId,
        EffectId receiverId,
        EffectId senderId)
    {
        if (_thrownException != null)
            throw _thrownException;

        await FetchMessagesOnce();

        var timeoutTask = timeout != null
            ? timeouts.AddTimeout(timeoutId, timeout.Value)
            : new TaskCompletionSource().Task;

        var maxWaitTask = timeout != null
            ? new TaskCompletionSource().Task
            : Task.Delay(settings.MessagesDefaultMaxWaitForCompletion);

        while (true)
        {
            if (_thrownException != null)
                throw _thrownException;

            var (matched, positionToRemoveIndex, pulseTask) = TryTakeMessage(predicate);
            if (matched != null)
            {
                var toRemoveId = new EffectId([-1, 0, positionToRemoveIndex]);
                effect.FlushlessUpserts(
                    new List<EffectResult>(
                    [
                        new EffectResult(toRemoveId, matched.Position, Alias: null),
                        new EffectResult(messageId, matched.MessageContentBytes, Alias: null),
                        new EffectResult(messageTypeId, matched.MessageTypeBytes, Alias: null),
                        new EffectResult(receiverId, matched.Receiver, Alias: null),
                        new EffectResult(senderId, matched.Sender, Alias: null),
                    ]).Concat(matched.IdempotencyKeyResult == null
                        ? []
                        : [matched.IdempotencyKeyResult])
                );

                timeouts.RemoveTimeout(timeoutId);
                return matched.Envelope;
            }

            await Task.WhenAny(pulseTask, timeoutTask, maxWaitTask);

            if (timeoutTask.IsCompleted)
                return null;

            if (maxWaitTask.IsCompleted)
            {
                await flowsManager.Suspend(storedId);
                return null;
            }
        }
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

    public void Dispose()
    {
        _disposed = true;
    }
}