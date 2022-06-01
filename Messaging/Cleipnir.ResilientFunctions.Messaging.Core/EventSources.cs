using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSources : IDisposable
{
    private readonly IEventStore _eventStore;

    private IDisposable? _storeSubscription;
    private bool _disposed;
    
    private readonly Dictionary<FunctionId, Dictionary<int, Action>> _observers = new();
    private int _nextObserverId;
    private bool _subscribedInDatabase;
    private readonly object _sync = new();

    public EventSources(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task Initialize()
    {
        lock (_sync)
        {
            if (_subscribedInDatabase || _disposed) return;
            _subscribedInDatabase = true;
        }
        
        var subscription = await _eventStore.SubscribeToChanges(NotifyEventSourceOfChange);
        _storeSubscription = subscription;
        
        lock (_sync)
            if (!_disposed) return;
        
        subscription.Dispose();
    }

    private void NotifyEventSourceOfChange(FunctionId functionId)
    {
        List<Action> observers;
        lock (_sync)
        {
            if (!_observers.ContainsKey(functionId)) return;
            observers = _observers[functionId].Values.ToList();
        }

        foreach (var observer in observers)
            observer();
    }

    public async Task<EventSource> GetEventSource(FunctionId functionId)
    {
        int observerId; 
        var eventSource = new EventSource(functionId, _eventStore);
        
        lock (_sync)
        {
            observerId = _nextObserverId++;
            if (!_observers.ContainsKey(functionId)) _observers[functionId] = new Dictionary<int, Action>();
            _observers[functionId][observerId] = eventSource.HandleNotification;
        }

        await eventSource.Initialize(unsubscribeToChanges: () =>
        {
            lock (_sync)
                _observers[functionId].Remove(observerId);
        });

        return eventSource;
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
        }
            
        _storeSubscription?.Dispose();
    }
}