using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSource : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;
    private readonly EventSourceWriter _eventWriter;
    private readonly TimeSpan _pullFrequency;
    private readonly ISerializer _eventSerializer;
    
    private readonly HashSet<string> _idempotencyKeys = new();
    private int _atEventCount;
    private int _deliverOutstandingEventsIteration;
    private EventProcessingException? _thrownException;
    
    private readonly AsyncSemaphore _asyncSemaphore = new(maxParallelism: 1);
    private readonly object _sync = new();
    
    private volatile bool _disposed;

    public TimeoutProvider TimeoutProvider { get; }

    private ImmutableList<object> _existing = ImmutableList<object>.Empty;
    public IReadOnlyList<object> Existing
    {
        get
        {
            lock (_sync)
                if (_thrownException == null)
                    return _existing;
                else
                    throw _thrownException;
        }
    }
    
    private readonly ReplaySubject<object> _allSubject = new();
    public IObservable<object> All => _allSubject;
    
    public EventSource(
        FunctionId functionId, 
        IEventStore eventStore, 
        EventSourceWriter eventWriter,
        TimeoutProvider timeoutProvider,
        TimeSpan? pullFrequency, 
        ISerializer? eventSerializer)
    {
        _functionId = functionId;
        _eventStore = eventStore;
        _eventWriter = eventWriter;
        TimeoutProvider = timeoutProvider;
        _eventSerializer = eventSerializer ?? DefaultSerializer.Instance;
        _pullFrequency = pullFrequency ?? TimeSpan.FromMilliseconds(250);
    }

    public async Task Initialize()
    {
        await DeliverOutstandingEvents();
        _ = Task.Run(async () =>
        {
            while (!_disposed)
            {
                await Task.Delay(_pullFrequency);
                if (_disposed) return;
                await DeliverOutstandingEvents();
            }
        });
    }

    private async Task DeliverOutstandingEvents()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(EventSource));

        int prevDeliverOutstandingEventsIteration; 
        lock (_sync)
            prevDeliverOutstandingEventsIteration = _deliverOutstandingEventsIteration;

        using var _ = await _asyncSemaphore.Take();
        lock (_sync)
            if (_thrownException != null)
                throw _thrownException;
            else if (_deliverOutstandingEventsIteration > prevDeliverOutstandingEventsIteration)
                return;
            else  
                _deliverOutstandingEventsIteration++;
        
        try
        {
            var storedEvents = await _eventStore.GetEvents(_functionId, skip: _atEventCount);
            foreach (var storedEvent in storedEvents)
            {
                _atEventCount++;

                if (storedEvent.IdempotencyKey != null)
                    if (_idempotencyKeys.Contains(storedEvent.IdempotencyKey))
                        continue;
                    else
                        _idempotencyKeys.Add(storedEvent.IdempotencyKey);

                var deserialized = _eventSerializer
                    .DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);
                lock (_sync)
                    _existing = _existing.Add(deserialized);
                _allSubject.OnNext(deserialized);
            }
        }
        catch (Exception e)
        {
            var eventHandlingException = new EventProcessingException(e);
            lock (_sync)
                _thrownException = eventHandlingException;
                    
            _allSubject.OnError(eventHandlingException);
            throw eventHandlingException;
        }
    }

    public async Task Append(object @event, string? idempotencyKey = null)
    {
        await _eventWriter.Append(@event, idempotencyKey);
        await DeliverOutstandingEvents();
    }

    public async Task Append(IEnumerable<EventAndIdempotencyKey> events)
    {
        await _eventWriter.Append(events);
        await DeliverOutstandingEvents();
    }

    public Task Sync() => DeliverOutstandingEvents();
    public void Dispose() => _disposed = true;
}

public record EventAndIdempotencyKey(object Event, string? IdempotencyKey = null);