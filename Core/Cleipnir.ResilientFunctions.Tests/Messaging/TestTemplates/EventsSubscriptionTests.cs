using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventSubscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null
        );
        var eventStore = functionStore.EventStore;

        var subscription = eventStore.SubscribeToEvents(functionId);

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