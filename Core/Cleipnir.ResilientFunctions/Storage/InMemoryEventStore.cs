using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

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

            var events = _events[functionId];
            foreach (var storedEvent in storedEvents)
                if (storedEvent.IdempotencyKey == null || events.All(e => e.IdempotencyKey != storedEvent.IdempotencyKey))
                    events.Add(storedEvent);
        }

        return Task.CompletedTask;
    }

    public Task Truncate(FunctionId functionId)
    {
        lock (_sync)
            _events[functionId] = new List<StoredEvent>();

        return Task.CompletedTask;
    }

    public Task Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        lock (_sync)
            _events[functionId] = storedEvents.ToList();

        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        lock (_sync)
        {
            if (!_events.ContainsKey(functionId))
                return Enumerable.Empty<StoredEvent>().ToTask();

            return _events[functionId].Skip(skip).ToList().AsEnumerable().ToTask();
        }
    }
}