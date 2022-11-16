using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSourceWriter
{
    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;
    private readonly ISerializer _serializer;

    public EventSourceWriter(FunctionId functionId, IEventStore eventStore, ISerializer eventSerializer)
    {
        _functionId = functionId;
        _eventStore = eventStore;
        _serializer = eventSerializer;
    }

    public async Task Append<TEvent>(TEvent @event, string? idempotencyKey = null) where TEvent : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeEvent(@event);
        await _eventStore.AppendEvent(
            _functionId,
            eventJson,
            eventType,
            idempotencyKey
        );
    }

    public async Task Append(IEnumerable<EventAndIdempotencyKey> events)
        => await _eventStore.AppendEvents(
                _functionId,
                storedEvents: events.Select(eventAndIdempotencyKey =>
                {
                    var (@event, idempotencyKey) = eventAndIdempotencyKey;
                    var (json, type) = _serializer.SerializeEvent(@event);
                    return new StoredEvent(json, type, idempotencyKey);
                })
            );

    public Task Truncate() => _eventStore.Truncate(_functionId);
}