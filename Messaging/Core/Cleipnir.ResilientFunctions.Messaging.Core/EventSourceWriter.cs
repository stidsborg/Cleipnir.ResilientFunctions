using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSourceWriter
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IEventStore _eventStore;
    private readonly IEventSerializer _eventSerializer;
    private readonly Func<FunctionInstanceId, Task>? _reInvoke;

    public EventSourceWriter(
        FunctionTypeId functionTypeId, 
        IEventStore eventStore, 
        IEventSerializer? eventSerializer, 
        Func<FunctionInstanceId, Task>? reInvoke)
    {
        _functionTypeId = functionTypeId;
        _eventStore = eventStore;
        _eventSerializer = eventSerializer ?? DefaultEventSerializer.Instance;
        _reInvoke = reInvoke;
    }

    public EventSourceInstanceWriter For(FunctionInstanceId functionInstanceId) => new(functionInstanceId, this);

    public async Task Append(
        FunctionInstanceId functionInstanceId, 
        object @event, 
        string? idempotencyKey, 
        bool awakeIfSuspended)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var eventJson = _eventSerializer.SerializeEvent(@event);
        var eventType = @event.GetType().SimpleQualifiedName();
        await _eventStore.AppendEvent(
            functionId,
            eventJson,
            eventType,
            idempotencyKey
        );
        if (awakeIfSuspended && _reInvoke != null)
            await _reInvoke.Invoke(functionInstanceId);
    }
    
    public async Task Append(FunctionInstanceId functionInstanceId, IEnumerable<EventAndIdempotencyKey> events, bool awakeIfSuspended)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        await _eventStore.AppendEvents(
            functionId,
            storedEvents: events.Select(eventAndIdempotencyKey =>
            {
                var (@event, idempotencyKey) = eventAndIdempotencyKey;
                return new StoredEvent(
                    EventJson: _eventSerializer.SerializeEvent(@event),
                    EventType: @event.GetType().SimpleQualifiedName(),
                    idempotencyKey
                );
            })
        );
        
        if (awakeIfSuspended && _reInvoke != null)
            await _reInvoke.Invoke(functionInstanceId);
    }
}