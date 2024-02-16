using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessagesSuscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredState, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messageStore = functionStore.MessageStore;

        var subscription = messageStore.SubscribeToMessages(functionId);

        var events = await subscription.PullNewEvents();
        events.ShouldBeEmpty();
        
        await messageStore.AppendMessage(
            functionId,
            messageJson: JsonExtensions.ToJson("hello world"),
            messageType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.PullNewEvents();
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello world");
        
        events = await subscription.PullNewEvents();
        events.ShouldBeEmpty();
        
        await messageStore.AppendMessage(
            functionId,
            messageJson: JsonExtensions.ToJson("hello universe"),
            messageType: typeof(string).SimpleQualifiedName()
        );

        events = await subscription.PullNewEvents();
        events.Count.ShouldBe(1);
        
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello universe");

        subscription.Dispose();
        
        await messageStore.AppendMessage(
            functionId,
            messageJson: JsonExtensions.ToJson("should not be received"),
            messageType: typeof(string).SimpleQualifiedName()
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
            Test.SimpleStoredState, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        var storedEvent1 = new StoredMessage(
            MessageJson: "hello".ToJson(),
            MessageType: typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "someIdempotencyKey"
        );
        var storedEvent2 = new StoredMessage(
            MessageJson: "world".ToJson(),
            MessageType: typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "someIdempotencyKey"
        );
        await functionStore.MessageStore.AppendMessage(functionId, storedEvent1);

        await Safe.Try(
            () => functionStore.MessageStore.AppendMessage(functionId, storedEvent2)
        );

        using var subscription = functionStore.MessageStore.SubscribeToMessages(functionId);

        var newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(2);
        newEvents[0].IdempotencyKey.ShouldBe("someIdempotencyKey");
        newEvents[0].DefaultDeserialize().ShouldBe("hello");
        newEvents[1].IdempotencyKey.ShouldBe("someIdempotencyKey");
        newEvents[1].DefaultDeserialize().ShouldBe("world");
    }
}