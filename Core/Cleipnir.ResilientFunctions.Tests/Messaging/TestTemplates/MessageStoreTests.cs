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
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "";
        const string msg2 = "";

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(msg1.ToJsonByteArray(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes())
        );
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(msg2.ToJsonByteArray(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes())
        );

        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBeNull();
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBeNull();
    }

    public abstract Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain();
    protected async Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson().ToUtf8Bytes(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson().ToUtf8Bytes(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        var storedEvent3 = new StoredMessage(msg3.ToJson().ToUtf8Bytes(), msg3.GetType().SimpleQualifiedName().ToUtf8Bytes(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson().ToUtf8Bytes(), msg4.GetType().SimpleQualifiedName().ToUtf8Bytes(), null);
        await messageStore.AppendMessage(functionId, storedEvent3);
        await messageStore.AppendMessage(functionId, storedEvent4);
        
        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
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
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson().ToUtf8Bytes(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson().ToUtf8Bytes(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson().ToUtf8Bytes(), msg3.GetType().SimpleQualifiedName().ToUtf8Bytes(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson().ToUtf8Bytes(), msg4.GetType().SimpleQualifiedName().ToUtf8Bytes(), null);
        await messageStore.ReplaceMessage(functionId, position: 0, storedEvent3).ShouldBeTrueAsync();
        await messageStore.ReplaceMessage(functionId, position: 1, storedEvent4).ShouldBeTrueAsync();
        
        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg3);
        events[0].IdempotencyKey.ShouldBe("3");
        events[1].DefaultDeserialize().ShouldBe(msg4);
        events[1].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task EventsAreReplacedWhenCountIsAsExpected();
    protected async Task EventsAreReplacedWhenCountIsAsExpected(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson().ToUtf8Bytes(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson().ToUtf8Bytes(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson().ToUtf8Bytes(), msg3.GetType().SimpleQualifiedName().ToUtf8Bytes(), "3");
        var storedEvent4 = new StoredMessage(msg4.ToJson().ToUtf8Bytes(), msg4.GetType().SimpleQualifiedName().ToUtf8Bytes(), null);
        await messageStore.ReplaceMessage(functionId, position: 0, storedEvent3).ShouldBeTrueAsync();
        await messageStore.ReplaceMessage(functionId, position: 1, storedEvent4).ShouldBeTrueAsync();
        
        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg3);
        events[0].IdempotencyKey.ShouldBe("3");
        events[1].DefaultDeserialize().ShouldBe(msg4);
        events[1].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task EventsAreNotReplacedWhenPositionIsNotAsExpected();
    protected async Task EventsAreNotReplacedWhenPositionIsNotAsExpected(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        
        var storedEvent1 = new StoredMessage(msg1.ToJson().ToUtf8Bytes(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson().ToUtf8Bytes(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(events => events.Count())
            .ShouldBeAsync(2);
        
        var storedEvent3 = new StoredMessage(msg3.ToJson().ToUtf8Bytes(), msg3.GetType().SimpleQualifiedName().ToUtf8Bytes(), "3");
        await messageStore.ReplaceMessage(functionId, position: 2, storedEvent3).ShouldBeFalseAsync();
        
        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBe("1");
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBe("2");
    }
    
    public abstract Task SkippedMessagesAreNotFetched();
    protected async Task SkippedMessagesAreNotFetched(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello world";
        const string msg2 = "hello universe";

        await messageStore.AppendMessage(functionId, new (msg1.ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));
        await messageStore.AppendMessage(functionId, new (msg2.ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));

        var events = (await messageStore.GetMessages(functionId, skip: 0)).Skip(1).ToList();
        events.Count.ShouldBe(1);
        events[0].DefaultDeserialize().ShouldBe(msg2);
        events[0].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task TruncatedMessagesContainsNoEvents();
    protected async Task TruncatedMessagesContainsNoEvents(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";

        var storedEvent1 = new StoredMessage(msg1.ToJson().ToUtf8Bytes(), msg1.GetType().SimpleQualifiedName().ToUtf8Bytes(), "1");
        var storedEvent2 = new StoredMessage(msg2.ToJson().ToUtf8Bytes(), msg2.GetType().SimpleQualifiedName().ToUtf8Bytes(), "2");
        await messageStore.AppendMessage(functionId, storedEvent1);
        await messageStore.AppendMessage(functionId, storedEvent2);

        await messageStore.Truncate(functionId);
        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.ShouldBeEmpty();
    }
    
    public abstract Task NoExistingMessagesCanBeTruncated();
    protected async Task NoExistingMessagesCanBeTruncated(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;
        
        await messageStore.Truncate(functionId);
        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.ShouldBeEmpty();
    }
    
    public abstract Task ExistingMessagesCanBeReplacedWithProvidedEvents();
    protected async Task ExistingMessagesCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );

        await messageStore.Truncate(functionId);
        await messageStore.AppendMessage(functionId, new("hello to you".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));
        await messageStore.AppendMessage(functionId, new("hello from me".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));

        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].MessageContent, Type.GetType(events[0].MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].MessageContent, Type.GetType(events[1].MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task NonExistingMessagesCanBeReplacedWithProvidedEvents();
    protected async Task NonExistingMessagesCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        await messageStore.Truncate(functionId);
        await messageStore.AppendMessage(functionId, new("hello to you".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));
        await messageStore.AppendMessage(functionId, new("hello from me".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes()));

        var events = (await messageStore.GetMessages(functionId, skip: 0)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].MessageContent, Type.GetType(events[0].MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].MessageContent, Type.GetType(events[1].MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages();
    protected async Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages(Task<IFunctionStore> functionStoreTask)
    {
        var event1 = new StoredMessage(
            JsonExtensions.ToJson("hello world").ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredMessage(
            "hello universe".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key"
        );
        
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        await messageStore.AppendMessage(functionId, event1);
        await messageStore.AppendMessage(functionId, event2);

        var events = await messageStore.GetMessages(functionId, skip: 0);
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
            "hello world".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredMessage(
            "hello universe".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key"
        );
        
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;

        await messageStore.AppendMessage(functionId, event1);
        await messageStore.AppendMessage(functionId, event2);

        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.Count.ShouldBe(2);
        events[0].IdempotencyKey.ShouldBe("idempotency_key");
        events[0].DefaultDeserialize().ShouldBe("hello world");
        events[1].IdempotencyKey.ShouldBe("idempotency_key");
        events[1].DefaultDeserialize().ShouldBe("hello universe");
    }

    public abstract Task FetchNonExistingEventsSucceeds();
    protected async Task FetchNonExistingEventsSucceeds(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;
        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.ShouldBeEmpty();
    }
    
    public abstract Task EventSubscriptionPublishesAppendedEvents();
    protected async Task EventSubscriptionPublishesAppendedEvents(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;
        
        var event1 = new StoredMessage(
            "hello world".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event1);

        var skip = 0;
        var newEvents = await messageStore.GetMessages(functionId, skip);
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageContent, storedEvent.MessageType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        skip += newEvents.Count;
        
        var event2 = new StoredMessage(
            "hello universe".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key_2"
        );
        await messageStore.AppendMessage(functionId, event2);

        newEvents = await messageStore.GetMessages(functionId, skip);
        newEvents.Count.ShouldBe(1);
        storedEvent = newEvents[0];
        @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageContent, storedEvent.MessageType);
        @event.ShouldBe("hello universe");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_2");
        skip += newEvents.Count;
        
        await messageStore.GetMessages(functionId, skip).SelectAsync(l => l.Count).ShouldBeAsync(0);
    }
    
    public abstract Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys();
    protected async Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        var messageStore = functionStore.MessageStore;
        
        var event1 = new StoredMessage(
            "hello world".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event1);

        var skip = 0;
        var newEvents = await messageStore.GetMessages(functionId, skip);
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeMessage(storedEvent.MessageContent, storedEvent.MessageType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        skip += newEvents.Count;
        
        var event2 = new StoredMessage(
            "hello universe".ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messageStore.AppendMessage(functionId, event2);

        newEvents = await messageStore.GetMessages(functionId, skip);
        newEvents.Count.ShouldBe(1);
        newEvents.Single().IdempotencyKey.ShouldBe("idempotency_key_1");
        skip += newEvents.Count;

        newEvents = await messageStore.GetMessages(functionId, skip);
        newEvents.Count.ShouldBe(0);
    }
}