using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests;

[TestClass]
public class EventStoreTests
{
    [TestMethod]
    public async Task AppendedMessagesCanBeFetchedAgain()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        using var eventStore = await Sql.CreateAndInitializeEventStore();

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
        events[0].Deserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBeNull();
        events[1].Deserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBeNull();
    }
    
    [TestMethod]
    public async Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        using var eventStore = await Sql.CreateAndInitializeEventStore();

        const string msg1 = "hello world";
        const string msg2 = "hello universe";
        var storedEvent1 = new StoredEvent(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");
        var storedEvent2 = new StoredEvent(msg2.ToJson(), msg2.GetType().SimpleQualifiedName(), "2");
        
        await eventStore.AppendEvents(functionId, new []{storedEvent1, storedEvent2});

        var events = (await eventStore.GetEvents(functionId, 0)).ToList();
        events.Count.ShouldBe(2);
        events[0].Deserialize().ShouldBe(msg1);
        events[0].IdempotencyKey.ShouldBe("1");
        events[1].Deserialize().ShouldBe(msg2);
        events[1].IdempotencyKey.ShouldBe("2");
    }
    
    [TestMethod]
    public async Task NotificationIsRaisedFromAppendedEvent()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var otherFunctionId = new FunctionId("TypeId", "InstanceId2");
        using var eventStore = await Sql.CreateAndInitializeEventStore();
        var changesDetected = new SyncedFlag();
        await eventStore.SubscribeToChanges(functionId, changesDetected.Raise);
        var otherFunctionChangesDetected = new SyncedFlag();
        await eventStore.SubscribeToChanges(otherFunctionId, otherFunctionChangesDetected.Raise);

        await Task.Delay(10);
        changesDetected.IsRaised.ShouldBeFalse();
        otherFunctionChangesDetected.IsRaised.ShouldBeFalse();

        const string msg1 = "hello world";
        var storedEvent1 = new StoredEvent(msg1.ToJson(), msg1.GetType().SimpleQualifiedName(), "1");

        await eventStore.AppendEvent(functionId, storedEvent1);

        await changesDetected.WaitForRaised();
        await Task.Delay(10);
        otherFunctionChangesDetected.IsRaised.ShouldBeFalse();
    }
}