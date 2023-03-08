using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventSubscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IEventStore> eventStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var eventStore = await eventStoreTask;

        var subscription = await eventStore.SubscribeToEvents(functionId);

        var events = await subscription.Pull();
        events.ShouldBeEmpty();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("hello world"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.Pull();
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeEvent(events[0].EventJson, events[0].EventType)
            .ShouldBe("hello world");
        
        events = await subscription.Pull();
        events.ShouldBeEmpty();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("hello universe"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.Pull();
        events.Count.ShouldBe(1);
        
        DefaultSerializer
            .Instance
            .DeserializeEvent(events[0].EventJson, events[0].EventType)
            .ShouldBe("hello universe");

        await subscription.DisposeAsync();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("should not be received"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.Pull();
        events.ShouldBeEmpty();
    }
}