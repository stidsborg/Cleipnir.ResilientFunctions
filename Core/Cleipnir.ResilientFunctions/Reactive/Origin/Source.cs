using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

public class Source : IReactiveChain<object>
{
    private bool _completed;

    private readonly ITimeouts _timeouts;
    private readonly SyncStore _syncStore;
    private readonly TimeSpan _defaultDelay;
    private readonly TimeSpan _defaultMaxWait;
    private readonly Func<bool> _isWorkflowRunning;
    private readonly Func<bool> _initialSyncPerformed;
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
    
    public Source(
        ITimeouts timeouts, 
        SyncStore syncStore, 
        TimeSpan defaultDelay, 
        TimeSpan defaultMaxWait, 
        Func<bool> isWorkflowRunning,
        Func<bool> initialSyncPerformed
    )
    {
        _timeouts = timeouts;
        _syncStore = syncStore;
        _defaultDelay = defaultDelay;
        
        _isWorkflowRunning = isWorkflowRunning;
        _initialSyncPerformed = initialSyncPerformed;
        _defaultMaxWait = defaultMaxWait;
    }

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError)
    {
        var subscription = new SourceSubscription(
            onNext, onCompletion, onError,
            source: this,
            _emittedEvents, _syncStore, _initialSyncPerformed, _isWorkflowRunning, _timeouts,
            _defaultDelay, _defaultMaxWait
        );

        return subscription;
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