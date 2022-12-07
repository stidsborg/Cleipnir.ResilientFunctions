using System;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISource<T> : IStream<T>
{
    void Emit(T toEmit);
    void SignalError(Exception exception);
    void SignalCompletion();
}

public class Source<T> : ISource<T>
{
    private readonly Dictionary<int, Subscription> _subscriptions = new();
    private int _nextSubscriptionId;
    private bool _completed;

    private readonly EmittedEvents _emittedEvents = new();
    private readonly object _sync = new();

    public int TotalEventCount
    {
        get
        {
            lock (_sync) return _emittedEvents.Count;
        }
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        lock (_sync)
        {
            var subscriptionId = _nextSubscriptionId++;
            var subscription = 
                new Subscription(
                    _emittedEvents, 
                    onNext, 
                    onCompletion, 
                    onError,
                    unsubscribeToEvents: () =>
                    {
                        lock (_sync)
                            _subscriptions.Remove(subscriptionId);
                    }
                );

            _subscriptions[subscriptionId] = subscription;
            return subscription;
        }
    }

    public void Emit(T toEmit)
    {
        List<Subscription> subscriptions;
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();

            var emittedEvent = new EmittedEvent(toEmit, completion: false, emittedException: null);
            _emittedEvents.Append(emittedEvent);
            subscriptions = _subscriptions.Values.ToList();
        }

        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }
    
    public void Emit(IEnumerable<T> toEmits)
    {
        List<Subscription> subscriptions;
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();

            var emittedEvents = toEmits
                .Select(toEmit => new EmittedEvent(toEmit, completion: false, emittedException: null))
                .ToList();
            
            _emittedEvents.Append(emittedEvents);
            subscriptions = _subscriptions.Values.ToList();
        }

        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    public void SignalError(Exception exception)
    {
        List<Subscription> subscriptions;

        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();
            _completed = true;

            var emittedEvent = new EmittedEvent(default, completion: false, emittedException: exception);
            _emittedEvents.Append(emittedEvent);
            
            subscriptions = _subscriptions.Values.ToList();
        }
        
        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    public void SignalCompletion()
    {
        List<Subscription> subscriptions;
        
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();
            _completed = true;
            
            var emittedEvent = new EmittedEvent(default, completion: true, emittedException: null);
            _emittedEvents.Append(emittedEvent);
            
            subscriptions = _subscriptions.Values.ToList();
        }
        
        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    private class Subscription : ISubscription
    {
        private Action<T> OnNext { get; }
        private Action OnCompletion { get; }
        private Action<Exception> OnError { get; }

        private bool _disposed;
        private bool _started;
        private bool _replayed;
        private bool _emittingEvents;
        private readonly EmittedEvents _emittedEvents;
        private int _skip;
        private readonly object _sync = new();

        public int EventSourceTotalCount => _emittedEvents.Count;
        private Action UnsubscribeToEvents { get; }

        public Subscription(EmittedEvents emittedEvents, Action<T> onNext, Action onCompletion, Action<Exception> onError, Action unsubscribeToEvents)
        {
            _emittedEvents = emittedEvents;
            
            OnNext = onNext;
            OnCompletion = onCompletion;
            OnError = onError;

            UnsubscribeToEvents = unsubscribeToEvents;
        }

        public void Start()
        {
            lock (_sync)
                if (_replayed)
                    throw new InvalidOperationException("Cannot start a replayed subscription");
                else    
                    _started = true;
            
            DeliverOutstandingEvents();  
        }

        public void ReplayUntil(int count)
        {
            lock (_sync)
                if (_started)
                    throw new InvalidOperationException("Cannot replay on a started subscription");
                else    
                    _replayed = true;

            var toEmits = _emittedEvents.GetNewEvents(skip: 0);
            toEmits = toEmits[..count];

            foreach (var toEmit in toEmits)
            {
                if (toEmit.Completion)
                    OnCompletion();
                else if (toEmit.EmittedException != null)
                    OnError(toEmit.EmittedException!);
                else
                    OnNext(toEmit.Event!);
            }
        }

        public void DeliverOutstandingEvents()
        {
            lock (_sync)
                if (_emittingEvents || _disposed || !_started)
                    return;
                else
                    _emittingEvents = true;
            try
            {
                while (true)
                {
                    var toEmits = _emittedEvents.GetNewEvents(_skip);
                    _skip += toEmits.Length;
                    
                    if (toEmits.IsEmpty)
                        lock (_sync)
                        {
                            _emittingEvents = false;
                            return;
                        }
                    
                    foreach (var toEmit in toEmits)
                    {
                        if (toEmit.Completion)
                            OnCompletion();
                        else if (toEmit.EmittedException != null)
                            OnError(toEmit.EmittedException!);
                        else
                            OnNext(toEmit.Event!);
                    }
                }
            }
            catch (Exception)
            {
                lock (_sync)
                {
                    _emittingEvents = false;
                    Dispose();
                }
                
                throw;
            }
        }

        public void Dispose()
        {
            lock (_sync) _disposed = true;
            UnsubscribeToEvents();  
        }
    }

    private struct EmittedEvent
    {
        public T? Event { get; }
        public bool Completion { get; }
        public Exception? EmittedException { get; }

        public EmittedEvent(T? @event, bool completion, Exception? emittedException)
        {
            Event = @event;
            Completion = completion;
            EmittedException = emittedException;
        }
    }

    private class EmittedEvents
    {
        private EmittedEvent[] _backingArray = new EmittedEvent[8];
        private int _count;
        private readonly object _sync = new();

        public int Count
        {
            get
            {
                lock (_sync) return _count;
            }
        }

        public void Append(EmittedEvent emittedEvent)
        {
            lock (_sync)
            {
                if (_backingArray.Length == _count)
                {
                    var prev = _backingArray;
                    var curr = new EmittedEvent[prev.Length * 2];
                    Array.Copy(sourceArray: prev, destinationArray: curr, length: prev.Length);
                    _backingArray = curr;
                }

                _backingArray[_count] = emittedEvent;
                _count++;   
            }
        }

        public void Append(IEnumerable<EmittedEvent> emittedEvents)
        {
            lock (_sync)
            {
                foreach (var emittedEvent in emittedEvents)
                {
                    if (_backingArray.Length == _count)
                    {
                        var prev = _backingArray;
                        var curr = new EmittedEvent[prev.Length * 2];
                        Array.Copy(sourceArray: prev, destinationArray: curr, length: prev.Length);
                        _backingArray = curr;
                    }

                    _backingArray[_count] = emittedEvent;
                    _count++;   
                }
            }
        }

        public Span<EmittedEvent> GetNewEvents(int skip)
        {
            lock (_sync)
                return _backingArray.AsSpan(start: skip, length: _count - skip);
        }
    }
}