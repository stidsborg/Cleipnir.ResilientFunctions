using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSources : IDisposable
{
    private readonly IEventStore _eventStore;
    public EventSources(IEventStore eventStore) => _eventStore = eventStore;

    public Task<EventSource> GetEventSource(string functionTypeId, string functionInstanceId)
        => GetEventSource(new FunctionId(functionTypeId, functionInstanceId));
    
    public async Task<EventSource> GetEventSource(FunctionId functionId)
    {
        var eventSource = new EventSource(functionId, _eventStore);
        await eventSource.Initialize();

        return eventSource;
    }

    public void Dispose() => _eventStore.Dispose();
}