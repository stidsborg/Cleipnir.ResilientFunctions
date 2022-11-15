using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSources
{
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly TimeSpan? _defaultPullFrequency;
    private readonly IEventSerializer? _eventSerializer;

    public EventSources(
        IEventStore eventStore,
        RFunctions? rFunctions,
        TimeSpan? defaultPullFrequency = null,
        IEventSerializer? eventSerializer = null
    )
    {
        _eventStore = eventStore;
        _rFunctions = rFunctions;
        _defaultPullFrequency = defaultPullFrequency;
        _eventSerializer = eventSerializer;
    }

    public FunctionTypeEventSources For(
        string functionTypeId, 
        TimeSpan? pullFrequency = null, 
        IEventSerializer? eventSerializer = null
    ) => For(new FunctionTypeId(functionTypeId), pullFrequency ?? _defaultPullFrequency, eventSerializer);    
    
    public FunctionTypeEventSources For(
        FunctionTypeId functionTypeId, 
        TimeSpan? pullFrequency = null, 
        IEventSerializer? eventSerializer = null
    ) => new FunctionTypeEventSources(
        _eventStore, 
        _rFunctions,
        functionTypeId, 
        pullFrequency ?? _defaultPullFrequency, 
        eventSerializer ?? _eventSerializer
    );
    
    public Task<EventSource> Get(
        string functionTypeId, 
        string functionInstanceId, 
        TimeSpan? pullFrequency = null,
        IEventSerializer? eventSerializer = null
    ) => Get(new FunctionId(functionTypeId, functionInstanceId), pullFrequency, eventSerializer);    
    
    public async Task<EventSource> Get(
        FunctionId functionId, 
        TimeSpan? pullFrequency = null, 
        IEventSerializer? eventSerializer = null
    )
    {
        var writer = new EventSourceWriter(
            functionId.TypeId,
            _eventStore,
            _rFunctions,
            eventSerializer ?? _eventSerializer
        ).For(functionId.InstanceId);
        
        var eventSource = new EventSource(
            functionId, 
            _eventStore,
            writer,
            pullFrequency, 
            eventSerializer ?? _eventSerializer);
        await eventSource.Initialize();

        return eventSource;
    }
}