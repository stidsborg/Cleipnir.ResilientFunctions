using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

internal class SubscriptionGroup : ISubscriptionGroup
{
    private ImmutableArray<Subscription> _subscriptions = ImmutableArray<Subscription>.Empty;
    private readonly EmittedEvents _emittedEvents;
    private int _skip;
    
    private bool _started;

    private readonly SyncStore _syncStore;
    private readonly Func<bool> _isWorkflowRunning;
    public bool IsWorkflowRunning => _isWorkflowRunning();
    public ISubscriptionGroup Group => this;
    public IReactiveChain<object> Source { get; }
    public ITimeoutProvider TimeoutProvider { get; }
    public TimeSpan DefaultMessageSyncDelay { get; }
    
    public SubscriptionGroup(
        IReactiveChain<object> source,
        EmittedEvents emittedEvents,
        SyncStore syncStore, 
        Func<bool> isWorkflowRunning,
        ITimeoutProvider timeoutProvider,
        TimeSpan defaultDelay)
    {
        Source = source;
        _emittedEvents = emittedEvents;
        _syncStore = syncStore;
        _isWorkflowRunning = isWorkflowRunning;
        TimeoutProvider = timeoutProvider;
        DefaultMessageSyncDelay = defaultDelay;
    }

    public Task SyncStore(TimeSpan maxSinceLastSynced) => _syncStore(maxSinceLastSynced);

    public InterruptCount PushMessages()
    {
        _started = true;

        var interruptCount = _emittedEvents.InterruptCount;
        var toEmits = _emittedEvents.GetEvents(_skip);
        _skip += toEmits.Length;

        //breath-first publishing of events
        foreach (var toEmit in toEmits) 
        foreach (var subscription in _subscriptions)
        {
            if (toEmit.Completion)
                subscription.SignalCompletion();
            else if (toEmit.EmittedException != null)
                subscription.SignalException(toEmit.EmittedException!);
            else
                subscription.SignalNext(toEmit.Event!);
        }

        return interruptCount;
    }
    
    public void Add(Action<object> onNext, Action onCompletion, Action<Exception> onError)
    {
        if (_started)
            throw new InvalidOperationException("Cannot add subscription to subscription group that has already emitted events");
        
        var subscription = new Subscription(onNext, onError, onCompletion);
        _subscriptions = _subscriptions.Add(subscription);
        
    }
    
    private record Subscription(Action<object> SignalNext, Action<Exception> SignalException, Action SignalCompletion);
}