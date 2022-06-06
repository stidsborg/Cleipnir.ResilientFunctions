using System.Collections.Immutable;
using System.Reactive.Subjects;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

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

    private bool _newEventsFlag;
    private bool _workerExecuting;
    private readonly HashSet<string> _idempotencyKeys = new();
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
        await DeliverOutstandingEvents();
    }

    internal void HandleNotification() => Task.Run(DeliverOutstandingEvents);
    
    private async Task DeliverOutstandingEvents()
    {
        Start:
        int count;
        lock (_sync)
        {
            if (_workerExecuting)
            {
                _newEventsFlag = true;
                return;
            }

            _workerExecuting = true;
            _newEventsFlag = false;
            count = _count;
        }

        var storedEvents = await _eventStore.GetEvents(_functionId, count);
        foreach (var storedEvent in storedEvents)
        {
            if (storedEvent.IdempotencyKey != null)
                if (_idempotencyKeys.Contains(storedEvent.IdempotencyKey))
                    continue;
                else
                    _idempotencyKeys.Add(storedEvent.IdempotencyKey);

            var deserialized = storedEvent.Deserialize();
            _existing = _existing.Add(deserialized);
            _allSubject.OnNext(deserialized);
            _count++;
        }

        lock (_sync)
        {
            _workerExecuting = false;
            if (_newEventsFlag) goto Start;
        }
    }

    public async Task Emit(object @event, string? idempotencyKey = null)
    {
        var json = JsonSerializer.Serialize(@event, @event.GetType());
        var type = @event.GetType().SimpleQualifiedName();
        await _eventStore.AppendEvent(_functionId, json, type, idempotencyKey);
        await DeliverOutstandingEvents();
    }

    public void Dispose() => _unsubscribeToChanges?.Invoke();
}