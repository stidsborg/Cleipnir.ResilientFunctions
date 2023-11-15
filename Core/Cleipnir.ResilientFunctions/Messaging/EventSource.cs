using System;
using System.Collections.Generic;
using System.Linq;
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
    public TimeoutProvider TimeoutProvider { get; }
    public IReactiveChain<object> Source => _eventPullerAndEmitter.Source;
    public IEnumerable<object> Existing => _eventPullerAndEmitter.Source.Existing;
    
    private readonly EventSourceWriter _eventWriter;
    private readonly EventsPullerAndEmitter _eventPullerAndEmitter;
    
    public EventSource(
        FunctionId functionId,
        IReadOnlyList<StoredEvent>? initialEvents,
        IEventStore eventStore, 
        EventSourceWriter eventWriter,
        TimeoutProvider timeoutProvider,
        TimeSpan? pullFrequency, 
        ISerializer serializer)
    {
        _eventWriter = eventWriter;
        TimeoutProvider = timeoutProvider;
        
        _eventPullerAndEmitter = new EventsPullerAndEmitter(
            functionId,
            initialEvents,
            pullFrequency ?? TimeSpan.FromMilliseconds(250),
            eventStore,
            serializer,
            timeoutProvider
        );
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

    public Task Sync() => _eventPullerAndEmitter.PullEvents();

    public void Dispose() => _eventPullerAndEmitter.Dispose();

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null) 
        => _eventPullerAndEmitter.Source.Subscribe(onNext, onCompletion, onError, subscriptionGroupId);

    private class EventsPullerAndEmitter : IDisposable
    {
        private readonly TimeSpan _delay;
        private readonly EventsSubscription _eventsSubscription;
        private readonly ISerializer _serializer;

        public Source Source { get; }
        
        private Exception? _thrownException;
        private int _subscribers;
        private bool _running;
        private volatile bool _disposed;
        private int _toSkip;
        
        private readonly AsyncSemaphore _semaphore = new(maxParallelism: 1);
        private readonly object _sync = new();
        
        public EventsPullerAndEmitter(
            FunctionId functionId, 
            IReadOnlyList<StoredEvent>? initialEvents,
            TimeSpan delay, 
            IEventStore eventStore, ISerializer serializer, ITimeoutProvider timeoutProvider)
        {
            _delay = delay;

            _serializer = serializer;

            Source = new Source(
                timeoutProvider, 
                onSubscriptionCreated: SubscriberAdded,
                onSubscriptionRemoved: SubscriberRemoved
            );

            if (initialEvents != null)
            { 
                Source.SignalNext(
                    initialEvents.Select(se => serializer.DeserializeEvent(se.EventJson, se.EventType))
                );
                _toSkip = initialEvents.Count;
            }
                
            
            _eventsSubscription = eventStore.SubscribeToEvents(functionId);
        }
        
        private async Task StartPullEventLoop()
        {
            lock (_sync)
                if (_running)
                    return;
                else 
                    _running = true;
            
            try
            {
                while (true)
                {
                    await Task.Delay(_delay);

                    lock (_sync)
                        if (_subscribers == 0)
                        {
                            _running = false;
                            return;
                        }
                    
                    await PullEvents();
                }                
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private void SubscriberAdded()
        {
            lock (_sync)
                _subscribers++;
            
            Task.Run(StartPullEventLoop);
        }

        private void SubscriberRemoved()
        {
            lock (_sync)
                _subscribers--;
        }

        public async Task PullEvents()
        {
            lock (_sync)
                if (_disposed)
                    throw new ObjectDisposedException(nameof(EventSource));
            
            using var @lock = await _semaphore.Take();
            if (_thrownException != null)
                throw new EventProcessingException(_thrownException);
            
            try
            {
                var storedEvents = await _eventsSubscription.PullNewEvents();
                
                if (_toSkip != 0)
                {
                    storedEvents = storedEvents.Skip(_toSkip).ToList();
                    _toSkip = 0;
                }
                    
                var events = storedEvents.Select(
                    storedEvent => _serializer.DeserializeEvent(storedEvent.EventJson, storedEvent.EventType)
                );
                
                Source.SignalNext(events);
            }
            catch (Exception e)
            {
                var eventHandlingException = new EventProcessingException(e);
                _thrownException = e;
                
                Source.SignalError(eventHandlingException);
                
                throw eventHandlingException;
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _eventsSubscription.Dispose();
        }  
    }
}