using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public static class EventStoreExtensions
{
    public static Task<EventSource> GetEventSource(
        this IEventStore eventStore, string functionTypeId, string functionInstanceId, TimeSpan? pullFrequency = null)
        => GetEventSource(eventStore, new FunctionId(functionTypeId, functionInstanceId), pullFrequency);    
    public static async Task<EventSource> GetEventSource(
        this IEventStore eventStore, FunctionId functionId, TimeSpan? pullFrequency = null)
    {
        var eventSource = new EventSource(functionId, eventStore: eventStore, pullFrequency);
        await eventSource.Initialize();

        return eventSource;
    }
}