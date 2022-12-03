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
    
    private readonly List<EmittedEvent> _emittedEvents = new();
    private readonly object _sync = new();

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
                    disposer: () =>
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
            _emittedEvents.Add(emittedEvent);
            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(emittedEvent);  
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
            
            _emittedEvents.AddRange(emittedEvents);
            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvents(emittedEvents);  
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
            _emittedEvents.Add(emittedEvent);
            
            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(emittedEvent);            
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
            _emittedEvents.Add(emittedEvent);
            
            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(emittedEvent);  
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
        private bool _emittingEvents;
        private bool _started;
        private readonly Queue<EmittedEvent> _queue;
        private readonly object _sync = new();

        private Action Disposer { get; }

        public Subscription(List<EmittedEvent> emittedEvents, Action<T> onNext, Action onCompletion, Action<Exception> onError, Action disposer)
        {
            OnNext = onNext;
            OnCompletion = onCompletion;
            OnError = onError;

            Disposer = disposer;
            _queue = new Queue<EmittedEvent>(emittedEvents);
        }

        public void Start()
        {
            lock (_sync)
                _started = true;
            
            DeliverOutstandingEvents();  
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
                    EmittedEvent toEmit;
                    lock (_sync)
                        if (!_queue.TryDequeue(out toEmit) || _disposed)
                        {
                            _emittingEvents = false;
                            return;
                        }

                    if (toEmit.Completion)
                        OnCompletion();
                    else if (toEmit.EmittedException != null)
                        OnError(toEmit.EmittedException!);
                    else
                        OnNext(toEmit.Event!);
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

        public void EnqueueEvent(EmittedEvent @event)
        {
            lock (_sync)
                _queue.Enqueue(@event);
        }
        
        public void EnqueueEvents(IEnumerable<EmittedEvent> events)
        {
            lock (_sync)
                foreach (var @event in events)
                    _queue.Enqueue(@event);
        }

        public void Dispose()
        {
            lock (_sync) _disposed = true;
            Disposer();  
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
}