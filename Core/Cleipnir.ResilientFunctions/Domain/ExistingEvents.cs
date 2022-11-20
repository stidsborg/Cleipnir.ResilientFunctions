using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEvents : IEnumerable<object>
{
    private readonly List<EventAndIdempotencyKey> _events;
    public List<EventAndIdempotencyKey> EventsWithIdempotencyKeys => _events;

    public ExistingEvents(List<EventAndIdempotencyKey> events) => _events = events;

    public object this[int index]
    {
        get => _events[index].Event;
        set => _events[index] = new EventAndIdempotencyKey(Event: value, IdempotencyKey: null);
    }

    public void Clear() => _events.Clear();
    public void Add(object @event) => _events.Add(new EventAndIdempotencyKey(_events, IdempotencyKey: null));
    public void AddRange(IEnumerable<object> events) 
        => _events.AddRange(events.Select(e => new EventAndIdempotencyKey(e)));

    public void Replace(IEnumerable<object> events)
    {
        _events.Clear();
        AddRange(events);
    }

    public IEnumerator<object> GetEnumerator() => _events.Select(e => e.Event).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}