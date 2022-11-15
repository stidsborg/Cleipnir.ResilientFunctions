using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSourceWriter
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly ISerializer _serializer;

    public EventSourceWriter(
        FunctionTypeId functionTypeId, 
        IEventStore eventStore, 
        RFunctions? rFunctions,
        ISerializer? eventSerializer)
    {
        _functionTypeId = functionTypeId;
        _eventStore = eventStore;
        _rFunctions = rFunctions;
        _serializer = eventSerializer ?? DefaultSerializer.Instance;
    }

    public EventSourceInstanceWriter For(FunctionInstanceId functionInstanceId) => new(functionInstanceId, this);

    public async Task Append(FunctionInstanceId functionInstanceId, object @event, string? idempotencyKey = null, bool awakeIfPostponed = false)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (eventJson, eventType) = _serializer.SerializeEvent(@event);
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
    
    public async Task Append(FunctionInstanceId functionInstanceId, IEnumerable<EventAndIdempotencyKey> events, bool awakeIfPostponed = false)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        await _eventStore.AppendEvents(
            functionId,
            storedEvents: events.Select(eventAndIdempotencyKey =>
            {
                var (@event, idempotencyKey) = eventAndIdempotencyKey;
                var (json, type) = _serializer.SerializeEvent(@event);
                return new StoredEvent(json, type, idempotencyKey);
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