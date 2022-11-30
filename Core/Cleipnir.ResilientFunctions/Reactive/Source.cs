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

    public IDisposable Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        Subscription subscription;
        lock (_sync)
        {
            var subscriptionId = _nextSubscriptionId++;
            subscription = new Subscription(
                _emittedEvents, 
                onNext, 
                onCompletion, 
                onError,
                () =>
                {
                    lock (_sync)
                        _subscriptions.Remove(subscriptionId);
                });

            _subscriptions[subscriptionId] = subscription;
        }

        subscription.DeliverOutstandingEvents();
        return subscription;
    }

    public void Emit(T toEmit)
    {
        List<Subscription> subscriptions;
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();

            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(new EmittedEvent(toEmit, completion: false, emittedException: null));  
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

            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(new EmittedEvent(default, completion: false, exception));            
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

            subscriptions = _subscriptions.Values.ToList();
            foreach (var subscription in subscriptions)
                subscription.EnqueueEvent(new EmittedEvent(default, completion: true, emittedException: null));  
        }
        
        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    private class Subscription : IDisposable
    {
        public Action<T> OnNext { get; }
        public Action OnCompletion { get; }
        public Action<Exception> OnError { get; }

        private bool _disposed;
        private bool _emittingEvents;
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

        public void DeliverOutstandingEvents()
        {
            lock (_sync)
                if (_emittingEvents || _disposed)
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

    public struct EmittedEvent
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