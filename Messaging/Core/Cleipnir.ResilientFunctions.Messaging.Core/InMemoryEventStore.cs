using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class InMemoryEventStore : IEventStore
{
    private readonly Dictionary<FunctionId, List<StoredEvent>> _events = new();
    private readonly object _sync = new();
    
    public Task Initialize() => Task.CompletedTask;

    public Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
        => AppendEvents(functionId, new[] {storedEvent});

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        lock (_sync)
        {
            if (!_events.ContainsKey(functionId))
                _events[functionId] = new List<StoredEvent>();
            
            _events[functionId].AddRange(storedEvents);
        }

        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        lock (_sync)
        {
            if (!_events.ContainsKey(functionId))
                Enumerable.Empty<StoredEvent>().ToTask();

            return _events[functionId].ToList().AsEnumerable().ToTask();
        }
    }
}