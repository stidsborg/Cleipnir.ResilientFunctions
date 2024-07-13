using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

internal class SourceSubscription : ISubscription
{
    private readonly Action<object> _onNext;
    private readonly Action _onCompletion;
    private readonly Action<Exception> _onError;
    
    private readonly EmittedEvents _emittedEvents;
    private int _skip;

    private readonly SyncStore _syncStore;
    private readonly Func<bool> _initialSyncPerformed;
    private readonly Func<bool> _isWorkflowRunning;
    public bool IsWorkflowRunning => _isWorkflowRunning();
    public IReactiveChain<object> Source { get; }
    public ITimeoutProvider TimeoutProvider { get; }
    public TimeSpan DefaultMessageSyncDelay { get; }
    public TimeSpan DefaultMessageMaxWait { get; }

    public SourceSubscription(
        Action<object> onNext, Action onCompletion, Action<Exception> onError,
        IReactiveChain<object> source,
        EmittedEvents emittedEvents,
        SyncStore syncStore, 
        Func<bool> initialSyncPerformed,
        Func<bool> isWorkflowRunning,
        ITimeoutProvider timeoutProvider,
        TimeSpan defaultDelay,
        TimeSpan defaultMessageMaxWait
    )
    {
        Source = source;
        _onNext = onNext;
        _onCompletion = onCompletion;
        _onError = onError;
        _emittedEvents = emittedEvents;
        _syncStore = syncStore;
        _initialSyncPerformed = initialSyncPerformed;
        _isWorkflowRunning = isWorkflowRunning;
        TimeoutProvider = timeoutProvider;
        DefaultMessageSyncDelay = defaultDelay;
        DefaultMessageMaxWait = defaultMessageMaxWait;
    }

    public async Task Initialize()
    {
        if (!_initialSyncPerformed())
            await _syncStore(maxSinceLastSynced: TimeSpan.Zero);
    }

    public Task SyncStore(TimeSpan maxSinceLastSynced) => _syncStore(maxSinceLastSynced);

    public InterruptCount PushMessages()
    {
        var interruptCount = _emittedEvents.InterruptCount;
        var toEmits = _emittedEvents.GetEvents(_skip);
        _skip += toEmits.Length;
        
        foreach (var toEmit in toEmits) 
            if (toEmit.Completion)
                _onCompletion();
            else if (toEmit.EmittedException != null)
                _onError(toEmit.EmittedException!);
            else
                _onNext(toEmit.Event!);

        return interruptCount;
    }
}