using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSources
{
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly TimeSpan? _defaultPullFrequency;
    private readonly ISerializer? _eventSerializer;

    public EventSources(
        IEventStore eventStore,
        RFunctions? rFunctions,
        TimeSpan? defaultPullFrequency = null,
        ISerializer? eventSerializer = null
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
        ISerializer? eventSerializer = null
    ) => For(new FunctionTypeId(functionTypeId), pullFrequency ?? _defaultPullFrequency, eventSerializer);    
    
    public FunctionTypeEventSources For(
        FunctionTypeId functionTypeId, 
        TimeSpan? pullFrequency = null, 
        ISerializer? eventSerializer = null
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
        ISerializer? eventSerializer = null
    ) => Get(new FunctionId(functionTypeId, functionInstanceId), pullFrequency, eventSerializer);    
    
    public async Task<EventSource> Get(
        FunctionId functionId, 
        TimeSpan? pullFrequency = null, 
        ISerializer? eventSerializer = null
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