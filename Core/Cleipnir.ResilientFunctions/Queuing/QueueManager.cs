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

public class QueueManager : IDisposable
{
    private const int ReservedIdPrefix = -1;

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

    private readonly Lock _lock = new();

    private static readonly EffectId PendingDeletionsRoot = new([ReservedIdPrefix, 0]);
    private static EffectId          PendingDeletion(int index) => new([ReservedIdPrefix, 0, index]);
    private static readonly EffectId IdempotencyKeysRoot   = new([ReservedIdPrefix, -1]);

    private readonly List<MessageData> _toDeliver = new();
    private readonly HashSet<long> _fetchedPositions = new();

    private readonly IdempotencyKeys _idempotencyKeys;
    private int _nextToRemoveIndex = 0;
    private readonly AsyncSignal _interruptSignal = new();
    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1);
    private bool _initialized = false;
    private volatile bool _disposed;

    private volatile Exception? _thrownException = null;

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
        
        _idempotencyKeys = new IdempotencyKeys(IdempotencyKeysRoot, _effect, maxIdempotencyKeyCount, maxIdempotencyKeyTtl, _utcNow);
    }

    private async Task Initialize()
    {
        await _semaphoreSlim.WaitAsync();

        try
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(QueueManager)} has already been disposed");
            if (_initialized)
                return;

            _effect.RegisterQueueManager(this);
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

            _initialized = true;
        }
        finally
        {
            _semaphoreSlim.Release();
        }

        _ = Task.Run(FetchLoop);
    }

    public async Task<QueueClient> CreateQueueClient()
    {
        if (!_initialized)
            await Initialize();
        return new QueueClient(this, _serializer, _utcNow);
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
                    return;
                }
            }
        }
        finally
        {
            _interruptSignal.Fire();
            _semaphoreSlim.Release();
        }
    }

    private (MessageData? Matched, int PositionToRemoveIndex, Task PulseTask) TryTakeMessage(MessagePredicate predicate)
    {
        var interruptedSignal = _interruptSignal.Wait();

        lock (_lock)
        {
            for (var i = 0; i < _toDeliver.Count; i++)
                if (predicate(_toDeliver[i].Envelope))
                {
                    var matched = _toDeliver[i];
                    _toDeliver.RemoveAt(i);
                    var positionToRemoveIndex = _nextToRemoveIndex++;
                    _effect.FlushlessUpsert(PendingDeletionsRoot, _nextToRemoveIndex, alias: null);
                    return (matched, positionToRemoveIndex, interruptedSignal);
                }

            return (null, 0, interruptedSignal);
        }
    }

    private async Task FetchLoop()
    {
        while (!_disposed && _thrownException == null)
        {
            await FetchMessagesOnce();
            await Task.WhenAny(
                _flowState.InterruptSignal.Wait(),
                Task.Delay(_settings.MessagesPullFrequency));
        }
    }

    public async Task AfterFlush()
    {
        await _semaphoreSlim.WaitAsync();
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
            _semaphoreSlim.Release();
        }
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

        var isHardTimeout = timeout != null;
        var waitTask = timeout != null
            ? _timeouts.AddTimeout(timeoutId, timeout.Value)
            : Task.Delay(_settings.MessagesDefaultMaxWaitForCompletion);

        while (true)
        {
            if (_thrownException != null)
                throw _thrownException;

            var (matched, positionToRemoveIndex, interruptSignal) = TryTakeMessage(predicate);
            if (matched != null)
            {
                var toRemoveId = PendingDeletion(positionToRemoveIndex);
                _effect.FlushlessUpserts(
                [
                    new EffectResult(toRemoveId, matched.Position, Alias: null),
                    new EffectResult(messageId, matched.MessageContentBytes, Alias: null),
                    new EffectResult(messageTypeId, matched.MessageTypeBytes, Alias: null),
                    new EffectResult(receiverId, matched.Receiver, Alias: null),
                    new EffectResult(senderId, matched.Sender, Alias: null),
                ]);

                _timeouts.RemoveTimeout(timeoutId);
                return matched.Envelope;
            }

            _flowState.SubflowWaiting();
            await Task.WhenAny(interruptSignal, waitTask);
            var success = _flowState.ResumeSubflow();
            if (!success)
                await new TaskCompletionSource().Task;

            if (waitTask.IsCompleted)
                return isHardTimeout ? null : throw new SuspendInvocationException();
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

    public void Dispose() => _disposed = true;
}
