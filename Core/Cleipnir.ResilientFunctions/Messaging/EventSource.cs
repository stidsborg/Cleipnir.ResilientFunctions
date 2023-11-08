using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSource : IReactiveChain<object>, IDisposable
{
    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;
    private readonly EventSourceWriter _eventWriter;
    private readonly TimeSpan _pullFrequency;
    private readonly ISerializer _eventSerializer;
    
    private readonly HashSet<string> _idempotencyKeys = new();
    private int _deliverOutstandingEventsIteration;
    private EventProcessingException? _thrownException;
    
    private readonly AsyncSemaphore _asyncSemaphore = new(maxParallelism: 1);
    private readonly object _sync = new();
    
    private volatile bool _disposed;

    public TimeoutProvider TimeoutProvider { get; }
    
    private readonly Source _source;
    private EventsSubscription? _eventsSubscription;
    public IReactiveChain<object> Source => _source;
    public IEnumerable<object> Existing => _source.Existing;

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
        _source = new Source(
            timeoutProvider, 
            onNewSubscription: () =>
            {
                if (_source?.HasActiveSubscriptions ?? false)
                    _ = DeliverOutstandingEvents(deliverDespiteNoActiveSubscriptions: false);
            } 
        );
    }

    public async Task Initialize()
    {
        _eventsSubscription = _eventStore.SubscribeToEvents(_functionId);
        
        await DeliverOutstandingEvents(deliverDespiteNoActiveSubscriptions: true);
        
        _ = Task.Run(async () =>
        {
            while (!_disposed)
            {
                await Task.Delay(_pullFrequency);
                if (_disposed) return;
                await DeliverOutstandingEvents(deliverDespiteNoActiveSubscriptions: false);
            }
        });
    }

    private async Task DeliverOutstandingEvents(bool deliverDespiteNoActiveSubscriptions)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(EventSource));

        if (!deliverDespiteNoActiveSubscriptions && !_source.HasActiveSubscriptions)
            return;
        
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
            var storedEvents = await _eventsSubscription!.PullNewEvents();
            foreach (var storedEvent in storedEvents)
            {
                if (storedEvent.IdempotencyKey != null)
                    if (_idempotencyKeys.Contains(storedEvent.IdempotencyKey))
                        continue;
                    else
                        _idempotencyKeys.Add(storedEvent.IdempotencyKey);

                var deserialized = _eventSerializer
                    .DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);

                _source.SignalNext(deserialized);
            }
        }
        catch (Exception e)
        {
            var eventHandlingException = new EventProcessingException(e);
            lock (_sync)
                _thrownException = eventHandlingException;
                    
            _source.SignalError(eventHandlingException);
            throw eventHandlingException;
        }
    }

    public async Task AppendEvent(object @event, string? idempotencyKey = null)
    {
        await _eventWriter.AppendEvent(@event, idempotencyKey);
        await Sync();
    }

    public async Task AppendEvents(IEnumerable<EventAndIdempotencyKey> events)
    {
        await _eventWriter.AppendEvents(events);
        await Sync();
    }

    public Task Sync() => DeliverOutstandingEvents(deliverDespiteNoActiveSubscriptions: true);

    public void Dispose()
    {
        _disposed = true;
        _eventsSubscription?.DisposeAsync();
    }

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null) 
        => _source.Subscribe(onNext, onCompletion, onError, subscriptionGroupId);
}