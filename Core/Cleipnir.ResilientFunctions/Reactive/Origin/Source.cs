using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

public class Source : IReactiveChain<object>
{
    private bool _completed;

    private readonly ITimeoutProvider _timeoutProvider;
    private readonly SyncStore _syncStore;
    private readonly TimeSpan _defaultDelay;
    private readonly Func<bool> _isWorkflowRunning;
    private readonly EmittedEvents _emittedEvents = new();

    public IEnumerable<object> Existing
    {
        get
        {
            var emittedSoFar = _emittedEvents.GetEvents(skip: 0);
            var toReturn = new List<object>(emittedSoFar.Length);
            foreach (var emittedEvent in emittedSoFar)
                if (emittedEvent.Event != null)
                    toReturn.Add(emittedEvent.Event);
            
            return toReturn;
        }
    }
    
    public Source(ITimeoutProvider timeoutProvider)
    {
        _timeoutProvider = timeoutProvider;
        _defaultDelay = TimeSpan.FromMilliseconds(10);
        _syncStore = _ => new InterruptCount(0).ToTask();
        _isWorkflowRunning = () => true;
    }

    public Source(ITimeoutProvider timeoutProvider, SyncStore syncStore, TimeSpan defaultDelay, Func<bool> isWorkflowRunning)
    {
        _timeoutProvider = timeoutProvider;
        _syncStore = syncStore;
        _defaultDelay = defaultDelay;
        _isWorkflowRunning = isWorkflowRunning;
    }

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError, ISubscriptionGroup? addToSubscriptionGroup = null)
    {
        addToSubscriptionGroup ??= new SubscriptionGroup(source: this, _emittedEvents, _syncStore, _isWorkflowRunning, _timeoutProvider, _defaultDelay);
        addToSubscriptionGroup.Add(onNext, onCompletion, onError);

        return addToSubscriptionGroup;
    }
    
    public void SignalNext(object toEmit, InterruptCount interruptCount)
    {
        if (_completed) 
            throw new StreamCompletedException();

        _emittedEvents.InterruptCount = interruptCount;
        
        var emittedEvent = new EmittedEvent(toEmit, completion: false, emittedException: null);
        _emittedEvents.Append(emittedEvent);
    }
    
    public void SignalNext(IEnumerable<object> toEmits, InterruptCount interruptCount)
    {
        if (_completed) throw new StreamCompletedException();

        _emittedEvents.InterruptCount = interruptCount;
        
        var emittedEvents = toEmits
            .Select(toEmit => new EmittedEvent(toEmit, completion: false, emittedException: null))
            .ToList();
        
        _emittedEvents.Append(emittedEvents);
    }

    public void SignalError(Exception exception)
    {
        if (_completed) throw new StreamCompletedException();
        _completed = true;
        
        var emittedEvent = new EmittedEvent(default, completion: false, emittedException: exception);
        _emittedEvents.Append(emittedEvent);
    }

    public void SignalCompletion()
    {
        if (_completed) throw new StreamCompletedException();
        _completed = true;
            
        var emittedEvent = new EmittedEvent(default, completion: true, emittedException: null);
            _emittedEvents.Append(emittedEvent);
    }
}