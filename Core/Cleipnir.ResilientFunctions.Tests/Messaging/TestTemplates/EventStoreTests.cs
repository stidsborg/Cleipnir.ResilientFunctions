using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessageStoreTests
{
    public abstract Task AppendedMessagesCanBeFetchedAgain();
    protected async Task AppendedMessagesCanBeFetchedAgain(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "";
        const string msg2 = "";

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName())
        );
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName())
        );

        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBeNull();
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBeNull();
    }

    public abstract Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain();
    protected async Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        var storedEvent3 = new StoredMessage(msg3.ToJson(), msg3.GetType().SimpleQualifiedName(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson(), msg4.GetType().SimpleQualifiedName(), null);
        await messageStore.AppendMessage(functionId, storedEvent3);
        await messageStore.AppendMessage(functionId, storedEvent4);
        
        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(4);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBe("1");
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBe("2");
        events[2].DefaultDeserialize().ShouldBe(msg3);
        events[2].IdempotencyKey.ShouldBe("3");
        events[3].DefaultDeserialize().ShouldBe(msg4);
        events[3].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task EventsCanBeReplaced();
    protected async Task EventsCanBeReplaced(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson(), msg3.GetType().SimpleQualifiedName(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson(), msg4.GetType().SimpleQualifiedName(), null);
        await messageStore.ReplaceMessage(functionId, position: 0, storedEvent3).ShouldBeTrueAsync();
        await messageStore.ReplaceMessage(functionId, position: 1, storedEvent4).ShouldBeTrueAsync();
        
        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg3);
        events[0].IdempotencyKey.ShouldBe("3");
        events[1].DefaultDeserialize().ShouldBe(msg4);
        events[1].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task EventsAreReplacedWhenCountIsAsExpected();
    protected async Task EventsAreReplacedWhenCountIsAsExpected(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson(), msg3.GetType().SimpleQualifiedName(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson(), msg4.GetType().SimpleQualifiedName(), null);
        await messageStore.ReplaceMessage(functionId, position: 0, storedEvent3).ShouldBeTrueAsync();
        await messageStore.ReplaceMessage(functionId, position: 1, storedEvent4).ShouldBeTrueAsync();
        
        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg3);
        events[0].IdempotencyKey.ShouldBe("3");
        events[1].DefaultDeserialize().ShouldBe(msg4);
        events[1].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task EventsAreNotReplacedWhenPositionIsNotAsExpected();
    protected async Task EventsAreNotReplacedWhenPositionIsNotAsExpected(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson(), msg3.GetType().SimpleQualifiedName(), "3");
        await messageStore.ReplaceMessage(functionId, position: 2, storedEvent3).ShouldBeFalseAsync();
        
        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBe("1");
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBe("2");
    }
    
    public abstract Task SkippedMessagesAreNotFetched();
    protected async Task SkippedMessagesAreNotFetched(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello world";
        const string msg2 = "hello universe";

        await messageStore.AppendMessage(functionId, new (msg1.ToJson(), typeof(string).SimpleQualifiedName()));
        await messageStore.AppendMessage(functionId, new (msg2.ToJson(), typeof(string).SimpleQualifiedName()));

        var events = (await messageStore.GetMessages(functionId)).Skip(1).ToList();
        events.Count.ShouldBe(1);
        events[0].DefaultDeserialize().ShouldBe(msg2);
        events[0].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task TruncatedMessagesContainsNoEvents();
    protected async Task TruncatedMessagesContainsNoEvents(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";

        var storedEvent1 = new StoredMessage(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore.Truncate(functionId);
        var events = await messageStore.GetMessages(functionId);
        events.ShouldBeEmpty();
    }
    
    public abstract Task NoExistingMessagesCanBeTruncated();
    protected async Task NoExistingMessagesCanBeTruncated(Task<IFunctionStore> functionStoreTask)
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
        
        await messageStore.Truncate(functionId);
        var events = await messageStore.GetMessages(functionId);
        events.ShouldBeEmpty();
    }
    
    public abstract Task ExistingMessagesCanBeReplacedWithProvidedEvents();
    protected async Task ExistingMessagesCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
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

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );

        await messageStore.Truncate(functionId);
        await messageStore.AppendMessage(functionId, new("hello to you".ToJson(), typeof(string).SimpleQualifiedName()));
        await messageStore.AppendMessage(functionId, new("hello from me".ToJson(), typeof(string).SimpleQualifiedName()));

        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].MessageJson, Type.GetType(events[0].MessageType, throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].MessageJson, Type.GetType(events[1].MessageType, throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task NonExistingMessagesCanBeReplacedWithProvidedEvents();
    protected async Task NonExistingMessagesCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
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

        await messageStore.Truncate(functionId);
        await messageStore.AppendMessage(functionId, new("hello to you".ToJson(), typeof(string).SimpleQualifiedName()));
        await messageStore.AppendMessage(functionId, new("hello from me".ToJson(), typeof(string).SimpleQualifiedName()));

        var events = (await messageStore.GetMessages(functionId)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].MessageJson, Type.GetType(events[0].MessageType, throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].MessageJson, Type.GetType(events[1].MessageType, throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages();
    protected async Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages(Task<IFunctionStore> functionStoreTask)
    {
        var event1 = new StoredMessage(
            JsonExtensions.ToJson("hello world"),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredMessage(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        
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

        await messageStore.AppendMessage(functionId, event1);
        await messageStore.AppendMessage(functionId, event2);

        var events = await messageStore.GetMessages(functionId).ToListAsync();
        events.Count.ShouldBe(2);
        events[0].IdempotencyKey.ShouldBe("idempotency_key");
        events[0].DefaultDeserialize().ShouldBe("hello world");
        events[1].IdempotencyKey.ShouldBe("idempotency_key");
        events[1].DefaultDeserialize().ShouldBe("hello universe");
    }
    
    public abstract Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessagesUsingBulkInsertion();
    protected async Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessagesUsingBulkInsertion(Task<IFunctionStore> functionStoreTask)
    {
        var event1 = new StoredMessage(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredMessage(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        
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

        await messageStore.AppendMessage(functionId, event1);
        await messageStore.AppendMessage(functionId, event2);

        var events = await messageStore.GetMessages(functionId).ToListAsync();
        events.Count.ShouldBe(2);
        events[0].IdempotencyKey.ShouldBe("idempotency_key");
        events[0].DefaultDeserialize().ShouldBe("hello world");
        events[1].IdempotencyKey.ShouldBe("idempotency_key");
        events[1].DefaultDeserialize().ShouldBe("hello universe");
    }

    public abstract Task FetchNonExistingEventsSucceeds();
    protected async Task FetchNonExistingEventsSucceeds(Task<IFunctionStore> functionStoreTask)
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
        var events = await messageStore.GetMessages(functionId);
        events.ShouldBeEmpty();
    }
    
    public abstract Task EventSubscriptionPublishesAppendedEvents();
    protected async Task EventSubscriptionPublishesAppendedEvents(Task<IFunctionStore> functionStoreTask)
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
        
        var event1 = new StoredMessage(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event1);

        var newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageJson, storedEvent.MessageType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        
        var event2 = new StoredMessage(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_2"
        );
        await messageStore.AppendMessage(functionId, event2);
        
        newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(1);
        storedEvent = newEvents[0];
        @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageJson, storedEvent.MessageType);
        @event.ShouldBe("hello universe");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_2");

        await subscription.PullNewEvents().SelectAsync(l => l.Count).ShouldBeAsync(0);
    }
    
    public abstract Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys();
    protected async Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(Task<IFunctionStore> functionStoreTask)
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
        
        var event1 = new StoredMessage(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event1);

        var newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageJson, storedEvent.MessageType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        
        var event2 = new StoredMessage(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event2);
        
        newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(1);
        newEvents.Single().IdempotencyKey.ShouldBe("idempotency_key_1");
        
        newEvents = await subscription.PullNewEvents();
        newEvents.Count.ShouldBe(0);
    }
}