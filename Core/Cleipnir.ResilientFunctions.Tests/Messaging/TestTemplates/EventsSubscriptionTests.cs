using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventSubscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;

        var sync = new object();
        var events = new List<StoredEvent>();

        var subscription = await eventStore.SubscribeToEvents(
            functionId,
            callback: newEvents =>
            {
                lock (sync)
                    events.AddRange(newEvents);
            },
            pullFrequency: null
        );


        await eventStore.AppendEvent(
            functionId,
            eventJson: "hello world".ToJson(),
            eventType: typeof(string).SimpleQualifiedName()
        );
        
        await BusyWait.UntilAsync(() =>
        {
            lock (sync)
                return events.Count == 1;
        });
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: "hello universe".ToJson(),
            eventType: typeof(string).SimpleQualifiedName()
        );

        await BusyWait.UntilAsync(() =>
        {
            lock (sync)
                return events.Count == 2;
        });

        await subscription.DisposeAsync();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: "should not be received".ToJson(),
            eventType: typeof(string).SimpleQualifiedName()
        );

        await Task.Delay(100);
        
        lock (sync)
            events.Count.ShouldBe(2);

        var deserialized = events
            .Select(e => (string) DefaultSerializer.Instance.DeserializeEvent(e.EventJson, e.EventType))
            .ToList();
        
        deserialized[0].ShouldBe("hello world");
        deserialized[1].ShouldBe("hello universe");
    }
}