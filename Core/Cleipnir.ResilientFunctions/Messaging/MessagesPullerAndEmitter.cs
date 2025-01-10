using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessagesPullerAndEmitter
{
    private readonly ISerializer _serializer;

    private DateTime _lastSynced = default;
    private readonly StoredId _storedId;

    private readonly IMessageStore _messageStore;
    public Source Source { get; }

    private Exception? _thrownException;

    private readonly HashSet<string> _idempotencyKeys = new();
    private int _skip;

    private readonly AsyncSemaphore _semaphore = new(maxParallelism: 1);
    private readonly Lock _sync = new();

    private bool InitialSyncPerformed
    {
        get
        {
            lock (_sync)
                return _lastSynced != default;
        }
    }

    public MessagesPullerAndEmitter(
        StoredId storedId,
        TimeSpan defaultDelay,
        TimeSpan defaultMaxWait,
        Func<bool> isWorkflowRunning,
        IFunctionStore functionStore, ISerializer serializer, IRegisteredTimeouts registeredTimeouts)
    {
        _storedId = storedId;
        _messageStore = functionStore.MessageStore;
        
        _serializer = serializer;
        
        Source = new Source(
            registeredTimeouts,
            syncStore: PullEvents,
            defaultDelay,
            defaultMaxWait,
            isWorkflowRunning,
            initialSyncPerformed: () => InitialSyncPerformed
        );
    }

    public async Task PullEvents(TimeSpan maxSinceLastSynced)
    {
        lock (_sync)
            if (
                _lastSynced != default &&
                maxSinceLastSynced > TimeSpan.Zero &&
                DateTime.UtcNow - maxSinceLastSynced < _lastSynced
            ) return;

        using var @lock = await _semaphore.Take();
        if (_thrownException != null)
            throw new MessageProcessingException(_thrownException);
        if (DateTime.UtcNow - maxSinceLastSynced < _lastSynced)
            return;

        try
        {
            var storedMessages = await _messageStore.GetMessages(_storedId, _skip);
            
            _lastSynced = DateTime.UtcNow;
            _skip += storedMessages.Count;

            if (storedMessages.Count == 0)
                return;
            
            var filterStoredMessages = new List<StoredMessage>(storedMessages.Count);
            foreach (var storedMessage in storedMessages)
                if (storedMessage.IdempotencyKey == null || !_idempotencyKeys.Contains(storedMessage.IdempotencyKey))
                {
                    filterStoredMessages.Add(storedMessage);
                    if (storedMessage.IdempotencyKey != null)
                        _idempotencyKeys.Add(storedMessage.IdempotencyKey);
                }

            storedMessages = filterStoredMessages;

            var events = storedMessages.Select(
                storedEvent => _serializer.DeserializeMessage(storedEvent.MessageContent, storedEvent.MessageType)
            );

            Source.SignalNext(events);
        }
        catch (Exception e)
        {
            var eventHandlingException = new MessageProcessingException(e);
            _thrownException = e;

            Source.SignalError(eventHandlingException);

            throw eventHandlingException;
        }

        return;
    }
}