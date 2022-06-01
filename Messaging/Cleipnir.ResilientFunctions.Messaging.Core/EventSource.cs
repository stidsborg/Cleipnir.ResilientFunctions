using System.Collections.Immutable;
using System.Reactive.Subjects;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSource : IDisposable
{
    private ImmutableList<object> _existing = ImmutableList<object>.Empty;

    public IReadOnlyList<object> Existing
    {
        get
        {
            lock (_sync)
                return _existing;
        }
    }
    private readonly ReplaySubject<object> _allSubject = new();
    public IObservable<object> All => _allSubject;

    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;

    private Action? _unsubscribeToChanges;

    private int _count;
    private readonly object _sync = new();
    
    public EventSource(FunctionId functionId, IEventStore eventStore)
    {
        _functionId = functionId;
        _eventStore = eventStore;
    }

    public async Task Initialize(Action unsubscribeToChanges)
    {
        _unsubscribeToChanges = unsubscribeToChanges;
        
        var existingMessages = await _eventStore.GetEvents(_functionId, skip: 0);
        foreach (var existingMessage in existingMessages)
        {
            _existing = _existing.Add(existingMessage);
            _allSubject.OnNext(existingMessage);
            _count++;
        }
    }

    internal void HandleNotification()
    {
        Task.Run(async () =>
        {
            var newMessages = await _eventStore.GetEvents(_functionId, _count);
            lock (_sync)
            {
                foreach (var newMessage in newMessages)
                {
                    _existing = _existing.Add(newMessage);
                    _allSubject.OnNext(newMessage); //todo is this blocking on an awaiting reactive listener?
                    _count++;
                }   
            }
        });
    }

    public async Task Emit(object message)
    {
        await _eventStore.AppendEvent(_functionId, message);
    }

    public void Dispose() => _unsubscribeToChanges?.Invoke();
}