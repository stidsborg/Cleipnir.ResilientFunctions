using System;
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

public class QueueManager : IDisposable
{
    private readonly Effect _effect;
    private readonly FlowState _flowState;
    private readonly ISerializer _serializer;
    private readonly UtcNow _utcNow;
    private readonly SettingsWithDefaults _settings;
    private readonly FetchedMessages _fetchedMessages;

    private readonly SemaphoreSlim _semaphoreSlim = new(1);
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
        _effect = effect;
        _flowState = flowState;
        _serializer = serializer;
        _utcNow = utcNow;
        _settings = settings;
        _fetchedMessages = new FetchedMessages(
            flowId,
            storedId,
            messageStore,
            serializer,
            effect,
            flowState,
            unhandledExceptionHandler,
            timeouts,
            utcNow,
            settings,
            maxIdempotencyKeyCount,
            maxIdempotencyKeyTtl
        );
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
            await _fetchedMessages.Initialize();

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

    public Task FetchMessagesOnce()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(QueueManager)} is disposed already");
        return _fetchedMessages.FetchOnce();
    }

    public Task AfterFlush() => _fetchedMessages.AfterFlush();

    public async Task<Envelope?> Subscribe(
        MessagePredicate predicate,
        DateTime? timeout,
        EffectId timeoutId,
        EffectId messageId,
        EffectId messageTypeId,
        EffectId receiverId,
        EffectId senderId)
    {
        if (_fetchedMessages.ThrownException is { } pre)
            throw pre;

        await FetchMessagesOnce();

        var matched = await _fetchedMessages.WaitForMessageOrTimeout(timeoutId, predicate, timeout);

        if (_fetchedMessages.ThrownException is { } post)
            throw post;

        if (matched == null)
            return timeout != null ? null : throw new SuspendInvocationException();

        _effect.FlushlessUpserts(
        [
            new EffectResult(matched.ToRemoveId, matched.Message.Position, Alias: null),
            new EffectResult(messageId, matched.Message.MessageContentBytes, Alias: null),
            new EffectResult(messageTypeId, matched.Message.MessageTypeBytes, Alias: null),
            new EffectResult(receiverId, matched.Message.Receiver, Alias: null),
            new EffectResult(senderId, matched.Message.Sender, Alias: null),
        ]);

        return matched.Message.Envelope;
    }

    private async Task FetchLoop()
    {
        while (!_disposed && _fetchedMessages.ThrownException == null)
        {
            await FetchMessagesOnce();
            await Task.WhenAny(
                _flowState.InterruptSignal.Wait(),
                Task.Delay(_settings.MessagesPullFrequency));
        }
    }

    public void Dispose() => _disposed = true;
}
