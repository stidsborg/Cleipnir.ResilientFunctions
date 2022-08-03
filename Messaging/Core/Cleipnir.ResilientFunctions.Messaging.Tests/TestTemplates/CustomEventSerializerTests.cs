using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;
using Cleipnir.ResilientFunctions.Messaging.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.TestTemplates;

public abstract class CustomEventSerializerTests
{
    public abstract Task CustomEventSerializerIsUsedWhenSpecified();
    protected async Task CustomEventSerializerIsUsedWhenSpecified(Task<IEventStore> eventStoreTask)
    {
        var eventStore = await eventStoreTask;
        var eventSerializer = new EventSerializer();
        var eventSources = new EventSources(eventStore, eventSerializer: eventSerializer);
        using var eventSource = await eventSources.Get(
            functionTypeId: nameof(CustomEventSerializerTests),
            functionInstanceId: nameof(CustomEventSerializerIsUsedWhenSpecified)
        );

        await eventSource.Append("hello world");
        
        eventSerializer.EventToSerialize.Count.ShouldBe(1);
        eventSerializer.EventToSerialize[0].ShouldBe("hello world");
        
        eventSerializer.EventToDeserialize.Count.ShouldBe(1);
        var (eventJson, eventType) = eventSerializer.EventToDeserialize[0];
        var deserializedEvent = DefaultEventSerializer.Instance.DeserializeEvent(eventJson, eventType);
        deserializedEvent.ShouldBe("hello world");
    }

    private class EventSerializer : IEventSerializer
    {
        public SyncedList<object> EventToSerialize { get; } = new();
        public SyncedList<Tuple<string, string>> EventToDeserialize { get; }= new();
        
        public string SerializeEvent(object @event)
        {
            EventToSerialize.Add(@event);
            return DefaultEventSerializer.Instance.SerializeEvent(@event);
        }

        public object DeserializeEvent(string json, string type)
        {
            EventToDeserialize.Add(Tuple.Create(json, type));
            return DefaultEventSerializer.Instance.DeserializeEvent(json, type);
        }
    }
}