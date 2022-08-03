using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSources
{
    private readonly IEventStore _eventStore;
    private readonly TimeSpan? _defaultPullFrequency;
    private readonly IEventSerializer? _eventSerializer;

    public EventSources(
        IEventStore eventStore,
        TimeSpan? defaultPullFrequency = null,
        IEventSerializer? eventSerializer = null
    )
    {
        _eventStore = eventStore;
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
            eventSerializer ?? _eventSerializer,
            reInvoke: null
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