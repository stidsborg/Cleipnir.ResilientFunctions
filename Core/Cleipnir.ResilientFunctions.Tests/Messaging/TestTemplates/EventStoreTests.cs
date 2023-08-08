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

public abstract class EventStoreTests
{
    public abstract Task AppendedMessagesCanBeFetchedAgain();
    protected async Task AppendedMessagesCanBeFetchedAgain(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello world";
        const string msg2 = "hello universe";

        await eventStore.AppendEvent(
            functionId,
            msg1.ToJson(),
            msg1.GetType().SimpleQualifiedName()
        );
        
        await eventStore.AppendEvent(
            functionId,
            msg2.ToJson(),
            msg2.GetType().SimpleQualifiedName()
        );

        var events = (await eventStore.GetEvents(functionId)).ToList();
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
            Test.SimpleStoredScrapbook, 
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null
        );
        var eventStore = functionStore.EventStore;

        const string msg1 = "hello here";
        const string msg2 = "hello world";
        const string msg3 = "hello universe";
        const string msg4 = "hello multiverse";
        
        var storedEvent1 = new StoredEvent(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredEvent(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await eventStore.AppendEvents(functionId, new []{storedEvent1, storedEvent2});

        var storedEvent3 = new StoredEvent(msg3.ToJson(), msg3.GetType().SimpleQualifiedName(), "3");
        var storedEvent4 = new StoredEvent(msg4.ToJson(), msg4.GetType().SimpleQualifiedName(), null);
        await eventStore.AppendEvents(functionId, new []{storedEvent3, storedEvent4});
        
        var events = (await eventStore.GetEvents(functionId)).ToList();
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
    
    public abstract Task SkippedMessagesAreNotFetched();
    protected async Task SkippedMessagesAreNotFetched(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello world";
        const string msg2 = "hello universe";

        await eventStore.AppendEvents(
            functionId,
            new StoredEvent[]
            {
                new (msg1.ToJson(), typeof(string).SimpleQualifiedName()),
                new (msg2.ToJson(), typeof(string).SimpleQualifiedName())
            }
        );

        var events = (await eventStore.GetEvents(functionId)).Skip(1).ToList();
        events.Count.ShouldBe(1);
        events[0].DefaultDeserialize().ShouldBe(msg2);
        events[0].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task TruncatedEventSourceContainsNoEvents();
    protected async Task TruncatedEventSourceContainsNoEvents(Task<IFunctionStore> functionStoreTask)
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

        const string msg1 = "hello here";
        const string msg2 = "hello world";

        var storedEvent1 = new StoredEvent(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredEvent(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await eventStore.AppendEvents(functionId, new []{storedEvent1, storedEvent2});

        await eventStore.Truncate(functionId);
        var events = await eventStore.GetEvents(functionId);
        events.ShouldBeEmpty();
    }
    
    public abstract Task NoExistingEventSourceCanBeTruncated();
    protected async Task NoExistingEventSourceCanBeTruncated(Task<IFunctionStore> functionStoreTask)
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
        
        await eventStore.Truncate(functionId);
        var events = await eventStore.GetEvents(functionId);
        events.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEventSourceCanBeReplacedWithProvidedEvents();
    protected async Task ExistingEventSourceCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
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

        await eventStore.AppendEvent(
            functionId,
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName()
        );
        
        await eventStore.AppendEvent(
            functionId,
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName()
        );

        await eventStore.Truncate(functionId);
        await eventStore.AppendEvents(
            functionId,
            new StoredEvent[]
            {
                new("hello to you".ToJson(), typeof(string).SimpleQualifiedName()),
                new("hello from me".ToJson(), typeof(string).SimpleQualifiedName())
            }
        );

        var events = (await eventStore.GetEvents(functionId)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].EventJson, Type.GetType(events[0].EventType, throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].EventJson, Type.GetType(events[1].EventType, throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task NonExistingEventSourceCanBeReplacedWithProvidedEvents();
    protected async Task NonExistingEventSourceCanBeReplacedWithProvidedEvents(Task<IFunctionStore> functionStoreTask)
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

        await eventStore.Truncate(functionId);
        await eventStore.AppendEvents(
            functionId,
            new StoredEvent[]
            {
                new("hello to you".ToJson(), typeof(string).SimpleQualifiedName()),
                new("hello from me".ToJson(), typeof(string).SimpleQualifiedName())
            }
        );

        var events = (await eventStore.GetEvents(functionId)).ToList();
        events.Count.ShouldBe(2);
        var event1 = (string) JsonSerializer.Deserialize(events[0].EventJson, Type.GetType(events[0].EventType, throwOnError: true)!)!;
        var event2 = (string) JsonSerializer.Deserialize(events[1].EventJson, Type.GetType(events[1].EventType, throwOnError: true)!)!;
        
        event1.ShouldBe("hello to you");
        event2.ShouldBe("hello from me");
    }
    
    public abstract Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource();
    protected async Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(Task<IFunctionStore> functionStoreTask)
    {
        var event1 = new StoredEvent(
            JsonExtensions.ToJson("hello world"),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredEvent(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        
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

        await eventStore.AppendEvent(functionId, event1);
        await eventStore.AppendEvent(functionId, event2);

        var events = await TaskLinq.ToListAsync(eventStore.GetEvents(functionId));
        events.Count.ShouldBe(1);
        events[0].IdempotencyKey.ShouldBe("idempotency_key");
        events[0].DefaultDeserialize().ShouldBe("hello world");
    }
    
    public abstract Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion();
    protected async Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(Task<IFunctionStore> functionStoreTask)
    {
        var event1 = new StoredEvent(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        var event2 = new StoredEvent(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key"
        );
        
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

        await eventStore.AppendEvents(functionId, new [] {event1, event2});

        var events = await TaskLinq.ToListAsync(eventStore.GetEvents(functionId));
        events.Count.ShouldBe(1);
        events[0].IdempotencyKey.ShouldBe("idempotency_key");
        events[0].DefaultDeserialize().ShouldBe("hello world");
    }

    public abstract Task FetchNonExistingEventsSucceeds();
    protected async Task FetchNonExistingEventsSucceeds(Task<IFunctionStore> functionStoreTask)
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
        var events = await eventStore.GetEvents(functionId);
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
            Test.SimpleStoredScrapbook, 
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null
        );
        var eventStore = functionStore.EventStore;

        var subscription = await eventStore.SubscribeToEvents(functionId);
        
        var event1 = new StoredEvent(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await eventStore.AppendEvent(functionId, event1);

        var newEvents = await subscription.Pull();
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        
        var event2 = new StoredEvent(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_2"
        );
        await eventStore.AppendEvent(functionId, event2);
        
        newEvents = await subscription.Pull();
        newEvents.Count.ShouldBe(1);
        storedEvent = newEvents[0];
        @event = DefaultSerializer.Instance.DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);
        @event.ShouldBe("hello universe");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_2");

        await subscription.Pull().SelectAsync(l => l.Count).ShouldBeAsync(0);
    }
    
    public abstract Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys();
    protected async Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(Task<IFunctionStore> functionStoreTask)
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

        var subscription = await eventStore.SubscribeToEvents(functionId);
        
        var event1 = new StoredEvent(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await eventStore.AppendEvent(functionId, event1);

        var newEvents = await subscription.Pull();
        newEvents.Count.ShouldBe(1);
        var storedEvent = newEvents[0];
        var @event = DefaultSerializer.Instance.DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);
        @event.ShouldBe("hello world");
        storedEvent.IdempotencyKey.ShouldBe("idempotency_key_1");
        
        var event2 = new StoredEvent(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await eventStore.AppendEvent(functionId, event2);
        
        newEvents = await subscription.Pull();
        newEvents.Count.ShouldBe(0);
        
        newEvents = await subscription.Pull();
        newEvents.Count.ShouldBe(0);
    }
}