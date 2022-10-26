using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSourceWriter
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly IEventSerializer _eventSerializer;

    public EventSourceWriter(
        FunctionTypeId functionTypeId, 
        IEventStore eventStore, 
        RFunctions? rFunctions,
        IEventSerializer? eventSerializer)
    {
        _functionTypeId = functionTypeId;
        _eventStore = eventStore;
        _rFunctions = rFunctions;
        _eventSerializer = eventSerializer ?? DefaultEventSerializer.Instance;
    }

    public EventSourceInstanceWriter For(FunctionInstanceId functionInstanceId) => new(functionInstanceId, this);

    public async Task Append(FunctionInstanceId functionInstanceId, object @event, string? idempotencyKey, bool awakeIfPostponed)
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
        if (awakeIfPostponed && _rFunctions != null)
            try
            {
                var sf = await _rFunctions.FunctionStore.GetFunctionStatus(new FunctionId(_functionTypeId, functionInstanceId));
                if (sf == null || sf.Status != Status.Postponed) return;
                await _rFunctions.ScheduleReInvoke(
                    _functionTypeId.Value,
                    functionInstanceId.Value,
                    expectedEpoch: sf.Epoch
                );
            } catch (UnexpectedFunctionState) {}
    }
    
    public async Task Append(FunctionInstanceId functionInstanceId, IEnumerable<EventAndIdempotencyKey> events, bool awakeIfPostponed)
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
        
        if (awakeIfPostponed && _rFunctions != null)
            try
            {
                var sf = await _rFunctions.FunctionStore.GetFunctionStatus(new FunctionId(_functionTypeId, functionInstanceId));
                if (sf == null || sf.Status != Status.Postponed) return;
                await _rFunctions.ScheduleReInvoke(_functionTypeId.Value, functionInstanceId.Value, sf.Epoch);
            } catch (UnexpectedFunctionState) {}
    }

    public Task Truncate(FunctionInstanceId functionInstanceId) 
        => _eventStore.Truncate(new FunctionId(_functionTypeId, functionInstanceId));
}