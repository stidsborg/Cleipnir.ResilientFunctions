using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

public class Source : IReactiveChain<object>
{
    private readonly Dictionary<int, SubscriptionGroup> _subscriptionGroups = new();
    private int _nextSubscriptionId;
    private bool _completed;

    private readonly Action? _onSubscriptionCreated;
    private readonly Action? _onSubscriptionRemoved;

    private readonly ITimeoutProvider _timeoutProvider;
    private readonly EmittedEvents _emittedEvents = new();
    private readonly object _sync = new();

    public IEnumerable<object> Existing
    {
        get
        {
            var emittedSoFar = _emittedEvents.GetEvents(0);
            var toReturn = new List<object>(emittedSoFar.Length);
            foreach (var emittedEvent in emittedSoFar)
            {
                if (emittedEvent.Event != null)
                    toReturn.Add(emittedEvent.Event);
            }

            return toReturn;
        }
    }
    
    public Source(ITimeoutProvider timeoutProvider) => _timeoutProvider = timeoutProvider;

    public Source(ITimeoutProvider timeoutProvider, Action onSubscriptionCreated, Action onSubscriptionRemoved)
    {
        _timeoutProvider = timeoutProvider;
        _onSubscriptionCreated = onSubscriptionCreated;
        _onSubscriptionRemoved = onSubscriptionRemoved;
    }

    public ISubscription Subscribe(
        Action<object> onNext, 
        Action onCompletion, 
        Action<Exception> onError, 
        int? subscriptionGroupId = null)
    {
        ISubscription subscription;
        
        lock (_sync)
        {
            if (subscriptionGroupId != null)
                return _subscriptionGroups[subscriptionGroupId.Value].AddSubscription(onNext, onCompletion, onError);
            
            var chosenSubscriptionGroupId = _nextSubscriptionId++;
            var subscriptionGroup =
                new SubscriptionGroup(
                    chosenSubscriptionGroupId,
                    _emittedEvents,
                    _timeoutProvider,
                    source: this,
                    unsubscribeToEvents: id =>
                    {
                        bool success;
                        lock (_sync)
                            success = _subscriptionGroups.Remove(id);
                        
                        if (success)
                            _onSubscriptionRemoved?.Invoke();
                    }
                );
            _subscriptionGroups[chosenSubscriptionGroupId] = subscriptionGroup;
            subscription = subscriptionGroup.AddSubscription(onNext, onCompletion, onError);
        }
        
        _onSubscriptionCreated?.Invoke();
        return subscription;
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
}