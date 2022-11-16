using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventStoreTests
{
    public abstract Task AppendedMessagesCanBeFetchedAgain();
    protected async Task AppendedMessagesCanBeFetchedAgain(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;

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

        var events = (await eventStore.GetEvents(functionId, 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].DefaultDeserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBeNull();
        events[1].DefaultDeserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBeNull();
    }

    public abstract Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain();
    protected async Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;

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
        
        var events = (await eventStore.GetEvents(functionId, 0)).ToList();
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
    protected async Task SkippedMessagesAreNotFetched(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;

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

        var events = (await eventStore.GetEvents(functionId, 1)).ToList();
        events.Count.ShouldBe(1);
        events[0].DefaultDeserialize().ShouldBe(msg2);
        events[0].IdempotencyKey.ShouldBeNull();
    }
    
    public abstract Task TruncatedEventSourceContainsNoEvents();
    protected async Task TruncatedEventSourceContainsNoEvents(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;

        const string msg1 = "hello here";
        const string msg2 = "hello world";

        var storedEvent1 = new StoredEvent(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredEvent(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        await eventStore.AppendEvents(functionId, new []{storedEvent1, storedEvent2});

        await eventStore.Truncate(functionId);
        var events = await eventStore.GetEvents(functionId, skip: 0);
        events.ShouldBeEmpty();
    }
    
    public abstract Task NoExistingEventSourceCanBeTruncated();
    protected async Task NoExistingEventSourceCanBeTruncated(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        
        await eventStore.Truncate(functionId);
        var events = await eventStore.GetEvents(functionId, skip: 0);
        events.ShouldBeEmpty();
    }
}