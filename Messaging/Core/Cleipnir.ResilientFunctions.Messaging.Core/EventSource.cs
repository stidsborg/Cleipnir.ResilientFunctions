using System.Collections.Immutable;
using System.Reactive.Subjects;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;
using Cleipnir.ResilientFunctions.Watchdogs;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSource : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;
    private readonly TimeSpan _pullFrequency;
    private readonly IEventSerializer _eventSerializer;
    
    private readonly HashSet<string> _idempotencyKeys = new();
    private int _atEventCount;
    private int _deliverOutstandingEventsIteration;
    private EventProcessingException? _thrownException;
    
    private readonly AsyncSemaphore _asyncSemaphore = new(1);
    private readonly object _sync = new();
    
    private volatile bool _disposed;

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
        TimeSpan? pullFrequency, 
        IEventSerializer? eventSerializer)
    {
        _functionId = functionId;
        _eventStore = eventStore;
        _eventSerializer = eventSerializer ?? DefaultEventSerializer.Instance;
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

    public async Task Emit(object @event, string? idempotencyKey = null)
    {
        var json = _eventSerializer.SerializeEvent(@event);
        var type = @event.GetType().SimpleQualifiedName();
        await _eventStore.AppendEvent(_functionId, json, type, idempotencyKey);
        await DeliverOutstandingEvents();
    }

    public async Task Emit(IEnumerable<EventAndIdempotencyKey> events)
    {
        await _eventStore.AppendEvents(
            _functionId,
            storedEvents: events.Select(eventAndIdempotencyKey =>
            {
                var (@event, idempotencyKey) = eventAndIdempotencyKey;
                return new StoredEvent(
                    EventJson: _eventSerializer.SerializeEvent(@event),
                    EventType: @event.GetType().SimpleQualifiedName(),
                    idempotencyKey
                );
            })
        );
        await DeliverOutstandingEvents();
    }

    public Task Pull() => DeliverOutstandingEvents();

    public void Dispose() => _disposed = true;
}

public record EventAndIdempotencyKey(object Event, string? IdempotencyKey = null);