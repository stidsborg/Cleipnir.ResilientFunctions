using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSourceWriter
{
    private readonly FunctionId _functionId;
    private readonly IFunctionStore _functionStore;
    private readonly IEventStore _eventStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public EventSourceWriter(FunctionId functionId, IFunctionStore functionStore, ISerializer eventSerializer, ScheduleReInvocation scheduleReInvocation)
    {
        _functionId = functionId;
        _functionStore = functionStore;
        _eventStore = functionStore.EventStore;
        _serializer = eventSerializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public async Task AppendEvent<TEvent>(TEvent @event, string? idempotencyKey = null, bool reInvokeImmediatelyIfSuspended = true) where TEvent : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeEvent(@event);
        await _eventStore.AppendEvent(
            _functionId,
            eventJson,
            eventType,
            idempotencyKey
        );

        if (!reInvokeImmediatelyIfSuspended) return;

        var epoch = await _functionStore.IsFunctionSuspendedAndEligibleForReInvocation(_functionId);
        if (epoch != null)
            try
            {
                await _scheduleReInvocation(_functionId.InstanceId.Value, epoch);    
            } catch (UnexpectedFunctionState) {}
            
    }

    public async Task AppendEvents(IEnumerable<EventAndIdempotencyKey> events, bool reInvokeImmediatelyIfSuspended = true)
    {
        await _eventStore.AppendEvents(
            _functionId,
            storedEvents: events.Select(eventAndIdempotencyKey =>
            {
                var (@event, idempotencyKey) = eventAndIdempotencyKey;
                var (json, type) = _serializer.SerializeEvent(@event);
                return new StoredEvent(json, type, idempotencyKey);
            })
        );  
        
        if (!reInvokeImmediatelyIfSuspended) return;

        var epoch = await _functionStore.IsFunctionSuspendedAndEligibleForReInvocation(_functionId);
        if (epoch != null)
            try
            {
                await _scheduleReInvocation(_functionId.InstanceId.Value, epoch);    
            } catch (UnexpectedFunctionState) {}
    } 

    public Task Truncate() => _eventStore.Truncate(_functionId);
}