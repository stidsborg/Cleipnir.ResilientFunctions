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
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var eventStore = functionStore.EventStore;

        var subscription = eventStore.SubscribeToEvents(functionId);

        var events = await subscription.PullNewEvents();
        events.ShouldBeEmpty();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("hello world"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.PullNewEvents();
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeEvent(events[0].EventJson, events[0].EventType)
            .ShouldBe("hello world");
        
        events = await subscription.PullNewEvents();
        events.ShouldBeEmpty();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("hello universe"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.PullNewEvents();
        events.Count.ShouldBe(1);
        
        DefaultSerializer
            .Instance
            .DeserializeEvent(events[0].EventJson, events[0].EventType)
            .ShouldBe("hello universe");

        subscription.Dispose();
        
        await eventStore.AppendEvent(
            functionId,
            eventJson: JsonExtensions.ToJson("should not be received"),
            eventType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.PullNewEvents();
        events.ShouldBeEmpty();
    }
    
    public abstract Task EventsWithSameIdempotencyKeyAreFilterOut();
    protected async Task EventsWithSameIdempotencyKeyAreFilterOut(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        var storedEvent1 = new StoredEvent(
            EventJson: "hello".ToJson(),
            EventType: typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "someIdempotencyKey"
        );
        var storedEvent2 = new StoredEvent(
            EventJson: "world".ToJson(),
            EventType: typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "someIdempotencyKey"
        );
        await functionStore.EventStore.AppendEvent(functionId, storedEvent1);

        await Safe.Try(
            () => functionStore.EventStore.AppendEvent(functionId, storedEvent2)
        );

        using var subscription = functionStore.EventStore.SubscribeToEvents(functionId);

        var newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(1);
        newEvents[0].IdempotencyKey.ShouldBe("someIdempotencyKey");
        newEvents[0].DefaultDeserialize().ShouldBe("hello");
    }
}