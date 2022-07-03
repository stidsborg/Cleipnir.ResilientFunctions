using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class FunctionTypeEventSources
{
    private readonly IEventStore _eventStore;
    private readonly FunctionTypeId _functionTypeId;
    private readonly TimeSpan? _pullFrequency;

    public FunctionTypeEventSources(IEventStore eventStore, FunctionTypeId functionTypeId, TimeSpan? pullFrequency = null)
    {
        _eventStore = eventStore;
        _functionTypeId = functionTypeId;
        _pullFrequency = pullFrequency;
    }

    public Task<EventSource> Get(
        string functionInstanceId, 
        TimeSpan? pullFrequency = null
    ) => Get(new FunctionInstanceId(functionInstanceId), pullFrequency);    
    
    public async Task<EventSource> Get(FunctionInstanceId functionInstanceId, TimeSpan? pullFrequency = null)
    {
        var eventSource = new EventSource(
            new FunctionId(_functionTypeId, functionInstanceId),
            _eventStore,
            pullFrequency ?? _pullFrequency
        );
        await eventSource.Initialize();

        return eventSource;
    }
}