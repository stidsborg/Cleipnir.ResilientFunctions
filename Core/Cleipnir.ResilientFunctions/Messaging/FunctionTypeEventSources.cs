using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class FunctionTypeEventSources
{
    private readonly IEventStore _eventStore;
    private readonly RFunctions? _rFunctions;
    private readonly FunctionTypeId _functionTypeId;
    private readonly TimeSpan? _pullFrequency;
    private readonly ISerializer? _serializer;
    
    public EventSourceWriter Writer { get; }

    public FunctionTypeEventSources(
        IEventStore eventStore, 
        RFunctions? rFunctions,
        FunctionTypeId functionTypeId, 
        TimeSpan? pullFrequency,
        ISerializer? serializer)
    {
        _eventStore = eventStore;
        _rFunctions = rFunctions;
        _functionTypeId = functionTypeId;
        _pullFrequency = pullFrequency;
        _serializer = serializer;
        
        Writer = new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _rFunctions,
            _serializer
        );
    }

    public Task<EventSource> Get(
        string functionInstanceId, 
        TimeSpan? pullFrequency = null,
        ISerializer? eventSerializer = null
    ) => Get(new FunctionInstanceId(functionInstanceId), pullFrequency, eventSerializer);    
    
    public async Task<EventSource> Get(
        FunctionInstanceId functionInstanceId, 
        TimeSpan? pullFrequency = null,
        ISerializer? eventSerializer = null)
    {
        var eventWriter = new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _rFunctions,
            eventSerializer ?? _serializer
        ).For(functionInstanceId);
        
        var eventSource = new EventSource(
            new FunctionId(_functionTypeId, functionInstanceId),
            _eventStore,
            eventWriter,
            pullFrequency ?? _pullFrequency,
            eventSerializer ?? _serializer
        );
        await eventSource.Initialize();

        return eventSource;
    }
}