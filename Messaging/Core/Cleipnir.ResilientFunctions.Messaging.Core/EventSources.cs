using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSources
{
    private readonly IEventStore _eventStore;

    public EventSources(IEventStore eventStore) => _eventStore = eventStore;

    public FunctionTypeEventSources For(string functionTypeId, TimeSpan? pullFrequency = null) 
        => For(new FunctionTypeId(functionTypeId), pullFrequency);    
    
    public FunctionTypeEventSources For(FunctionTypeId functionTypeId, TimeSpan? pullFrequency = null)
        => new FunctionTypeEventSources(_eventStore, functionTypeId, pullFrequency);
    
    public Task<EventSource> Get(
        string functionTypeId, 
        string functionInstanceId, 
        TimeSpan? pullFrequency = null
    ) => Get(new FunctionId(functionTypeId, functionInstanceId), pullFrequency);    
    
    public async Task<EventSource> Get(FunctionId functionId, TimeSpan? pullFrequency = null)
    {
        var eventSource = new EventSource(functionId, _eventStore, pullFrequency);
        await eventSource.Initialize();

        return eventSource;
    }
}