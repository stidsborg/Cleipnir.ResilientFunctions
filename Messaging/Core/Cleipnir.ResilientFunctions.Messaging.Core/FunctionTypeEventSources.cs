using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class FunctionTypeEventSources
{
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly TimeSpan? _pullFrequency;
    private readonly IEventSerializer? _eventSerializer;
    
    public EventSourceWriter Writer { get; }

    public FunctionTypeEventSources(
        IEventStore eventStore, 
        RFunctions? rFunctions,
        FunctionTypeId functionTypeId, 
        TimeSpan? pullFrequency,
        IEventSerializer? eventSerializer)
    {
        _eventStore = eventStore;
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
        _pullFrequency = pullFrequency;
        _eventSerializer = eventSerializer;
        
        Writer = new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _rFunctions,
            _eventSerializer
        );
    }

    public Task<EventSource> Get(
        string functionInstanceId, 
        TimeSpan? pullFrequency = null,
        IEventSerializer? eventSerializer = null
    ) => Get(new FunctionInstanceId(functionInstanceId), pullFrequency, eventSerializer);    
    
    public async Task<EventSource> Get(
        FunctionInstanceId functionInstanceId, 
        TimeSpan? pullFrequency = null,
        IEventSerializer? eventSerializer = null)
    {
        var eventWriter = new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _rFunctions,
            eventSerializer ?? _eventSerializer
        ).For(functionInstanceId);
        
        var eventSource = new EventSource(
            new FunctionId(_functionTypeId, functionInstanceId),
            _eventStore,
            eventWriter,
            pullFrequency ?? _pullFrequency,
            eventSerializer ?? _eventSerializer
        );
        await eventSource.Initialize();

        return eventSource;
    }
}