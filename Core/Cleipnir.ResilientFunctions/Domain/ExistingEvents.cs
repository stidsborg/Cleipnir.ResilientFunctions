using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEvents : IEnumerable<object>
{
    private readonly FunctionId _functionId;
    private readonly List<EventAndIdempotencyKey> _events;
    private readonly IEventStore _eventStore;
    private readonly ISerializer _serializer;
    public List<EventAndIdempotencyKey> EventsWithIdempotencyKeys => _events;

    internal int ExistingCount { get; }
    public ExistingEvents(FunctionId functionId, List<EventAndIdempotencyKey> events, IEventStore eventStore, ISerializer serializer)
    {
        _functionId = functionId;
        _events = events;
        _eventStore = eventStore;
        _serializer = serializer;
        
        ExistingCount = _events.Count;
    }

    public object this[int index]
    {
        get => _events[index].Event;
        set => _events[index] = new EventAndIdempotencyKey(Event: value, IdempotencyKey: null);
    }

    public void Clear() => _events.Clear();
    public void Add(object @event) => _events.Add(new EventAndIdempotencyKey(@event, IdempotencyKey: null));
    public void AddRange(IEnumerable<object> events) 
        => _events.AddRange(events.Select(e => new EventAndIdempotencyKey(e)));

    public void Replace(IEnumerable<object> events)
    {
        _events.Clear();
        AddRange(events);
    }
    
    public async Task SaveChanges()
    {
        var storedEvents = _events.Select(eventAndIdempotencyKey =>
        {
            var (json, type) = _serializer.SerializeEvent(eventAndIdempotencyKey);
            return new StoredEvent(json, type, eventAndIdempotencyKey.IdempotencyKey);
        });

        await _eventStore.Replace(_functionId, storedEvents);
    }

    public IEnumerator<object> GetEnumerator() => _events.Select(e => e.Event).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}