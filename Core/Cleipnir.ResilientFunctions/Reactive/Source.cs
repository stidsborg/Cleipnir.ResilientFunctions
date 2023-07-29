using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISource : IReactiveChain<object>
{
    void SignalNext(object toEmit);
    void SignalError(Exception exception);
    void SignalCompletion();
}

public class Source : ISource
{
    private readonly Dictionary<int, SubscriptionGroup> _subscriptionGroups = new();
    private int _nextSubscriptionId;
    private bool _completed;

    private readonly ITimeoutProvider _timeoutProvider;
    private readonly EmittedEvents _emittedEvents = new();
    private readonly object _sync = new();

    public IEnumerable<object> Existing
    {
        get
        {
            var emittedSoFar = _emittedEvents.GetEvents(0);
            var toReturn = new List<object>();
            foreach (var emittedEvent in emittedSoFar)
            {
                if (emittedEvent.Event != null)
                    toReturn.Add(emittedEvent.Event);
            }

            return toReturn;
        }
    }



    public Source(ITimeoutProvider timeoutProvider) => _timeoutProvider = timeoutProvider;

    public ISubscription Subscribe(
        Action<object> onNext, 
        Action onCompletion, 
        Action<Exception> onError, 
        int? subscriptionGroupId = null)
    {
        lock (_sync)
        {
            if (subscriptionGroupId != null)
                return
                    _subscriptionGroups[subscriptionGroupId.Value]
                        .AddSubscription(onNext, onCompletion, onError);
            
            var chosenSubscriptionGroupId = _nextSubscriptionId++;
            var subscriptionGroup =
                new SubscriptionGroup(
                    chosenSubscriptionGroupId,
                    _emittedEvents,
                    _timeoutProvider,
                    source: this,
                    unsubscribeToEvents: id =>
                    {
                        lock (_sync)
                            _subscriptionGroups.Remove(id);
                    }
                );
            _subscriptionGroups[chosenSubscriptionGroupId] = subscriptionGroup;
            return subscriptionGroup.AddSubscription(onNext, onCompletion, onError);
        }
    }
    
    public void SignalNext(object toEmit)
    {
        List<SubscriptionGroup> subscriptions;
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();

            var emittedEvent = new EmittedEvent(toEmit, completion: false, emittedException: null);
            _emittedEvents.Append(emittedEvent);
            subscriptions = _subscriptionGroups.Values.ToList();
        }

        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }
    
    public void SignalNext(IEnumerable<object> toEmits)
    {
        List<SubscriptionGroup> subscriptions;
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();

            var emittedEvents = toEmits
                .Select(toEmit => new EmittedEvent(toEmit, completion: false, emittedException: null))
                .ToList();
            
            _emittedEvents.Append(emittedEvents);
            subscriptions = _subscriptionGroups.Values.ToList();
        }

        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    public void SignalError(Exception exception)
    {
        List<SubscriptionGroup> subscriptions;

        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();
            _completed = true;

            var emittedEvent = new EmittedEvent(default, completion: false, emittedException: exception);
            _emittedEvents.Append(emittedEvent);
            
            subscriptions = _subscriptionGroups.Values.ToList();
        }
        
        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    public void SignalCompletion()
    {
        List<SubscriptionGroup> subscriptions;
        
        lock (_sync)
        {
            if (_completed) throw new StreamCompletedException();
            _completed = true;
            
            var emittedEvent = new EmittedEvent(default, completion: true, emittedException: null);
            _emittedEvents.Append(emittedEvent);
            
            subscriptions = _subscriptionGroups.Values.ToList();
        }
        
        foreach (var subscription in subscriptions)
            subscription.DeliverOutstandingEvents();
    }

    private struct EmittedEvent
    {
        public object? Event { get; }
        public bool Completion { get; }
        public Exception? EmittedException { get; }

        public EmittedEvent(object? @event, bool completion, Exception? emittedException)
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

        public Span<EmittedEvent> GetEvents(int skip)
        {
            lock (_sync)
                return _backingArray.AsSpan(start: skip, length: _count - skip);
        }
    }

    private class SubscriptionGroup
    {
        private readonly int _subscriptionGroupId;
        private readonly Source _source;
        
        private bool _disposed;
        private bool _started;
        private bool _replayed;
        private bool _emittingEvents;
        private ImmutableArray<Subscription> _subscriptions = ImmutableArray<Subscription>.Empty;
        private readonly EmittedEvents _emittedEvents;
        private readonly ITimeoutProvider _timeoutProvider;
        private int _skip;
        private readonly object _sync = new();

        private Action<int> UnsubscribeToEvents { get; }

        public SubscriptionGroup(
            int subscriptionGroupId, 
            EmittedEvents emittedEvents,
            ITimeoutProvider timeoutProvider,
            Source source,
            Action<int> unsubscribeToEvents)
        {
            _subscriptionGroupId = subscriptionGroupId;
            _emittedEvents = emittedEvents;
            _timeoutProvider = timeoutProvider;
            UnsubscribeToEvents = unsubscribeToEvents;
            _source = source;
        }

        public void DeliverExistingAndFuture()
        {
            lock (_sync)
                if (_replayed)
                    throw new InvalidOperationException("Cannot start a replayed subscription");
                else
                    _started = true;

            DeliverOutstandingEvents();
        }

        public int DeliverExisting()
        {
            lock (_sync)
                if (_started)
                    throw new InvalidOperationException("Cannot replay on a started subscription");
                else
                    _replayed = true;

            var toEmits = _emittedEvents.GetEvents(skip: 0);

            foreach (var toEmit in toEmits)
            foreach (var subscription in _subscriptions)
            {
                if (toEmit.Completion)
                    subscription.OnCompletion();
                else if (toEmit.EmittedException != null)
                    subscription.OnError(toEmit.EmittedException!);
                else
                    subscription.OnNext(toEmit.Event!);
            }

            foreach (var subscription in _subscriptions)
                subscription.OnCompletion();
            
            return toEmits.Length;
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
                    var toEmits = _emittedEvents.GetEvents(_skip);
                    _skip += toEmits.Length;

                    if (toEmits.IsEmpty)
                        lock (_sync)
                        {
                            _emittingEvents = false;
                            return;
                        }

                    foreach (var toEmit in toEmits)
                    foreach (var subscription in _subscriptions)
                    {
                        if (toEmit.Completion)
                            subscription.OnCompletion();
                        else if (toEmit.EmittedException != null)
                            subscription.OnError(toEmit.EmittedException!);
                        else
                            subscription.OnNext(toEmit.Event!);
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

        public ISubscription AddSubscription(Action<object> onNext, Action onCompletion, Action<Exception> onError)
        {
            var subscription = new Subscription(onNext, onCompletion, onError, subscriptionGroup: this, _timeoutProvider, _source);
            _subscriptions = _subscriptions.Add(subscription);
            return subscription;
        }

        private void Unsubscribe(Subscription subscription)
        {
            lock (_sync)
            {
                _subscriptions = _subscriptions.Remove(subscription);
                if (_subscriptions.Length > 0)
                    return;
            }

            Dispose();
        }

        private void Dispose()
        {
            lock (_sync) _disposed = true;
            UnsubscribeToEvents(_subscriptionGroupId);
        }
        
        private class Subscription : ISubscription
        {
            public Action<object> OnNext { get; }
            public Action OnCompletion { get; }
            public Action<Exception> OnError { get; }
            
            public IReactiveChain<object> Source { get; }
            
            private readonly SubscriptionGroup _subscriptionGroup;
            
            public Subscription(
                Action<object> onNext, Action onCompletion, Action<Exception> onError,
                SubscriptionGroup subscriptionGroup,
                ITimeoutProvider timeoutProvider,
                IReactiveChain<object> source)
            {
                OnNext = onNext;
                OnCompletion = onCompletion;
                OnError = onError;
                Source = source;
                _subscriptionGroup = subscriptionGroup;
                TimeoutProvider = timeoutProvider;
            }
            
            public ITimeoutProvider TimeoutProvider { get; }
            public int SubscriptionGroupId => _subscriptionGroup._subscriptionGroupId;
            public void DeliverExistingAndFuture() => _subscriptionGroup.DeliverExistingAndFuture();
            public int DeliverExisting() => _subscriptionGroup.DeliverExisting();
            public void Dispose() => _subscriptionGroup.Unsubscribe(this);
        }
    }
}