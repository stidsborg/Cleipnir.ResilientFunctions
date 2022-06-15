using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests;

[TestClass]
public class EventSourcesTests
{
    [TestMethod]
    public async Task EventSourcesSunshineScenario()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await Sql.CreateAndInitializeEventStore();
        var eventSource = await eventStore.GetEventSource(functionId);

        async Task<object> FirstAsync() => await eventSource.All.FirstAsync();
        var task = FirstAsync();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.Emit("hello world");

        (await task).ShouldBe("hello world");
    }
    
    [TestMethod]
    public async Task SecondEventWithExistingIdempotencyKeyIsIgnored()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await Sql.CreateAndInitializeEventStore();
        var eventSource = await eventStore.GetEventSource(functionId);

        async Task<IList<object>> TakeTwo() => await eventSource.All.Take(2).ToList();
        var task = TakeTwo();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.Emit("hello world", "1");
        await eventSource.Emit("hello world", "1");
        await eventSource.Emit("hello universe");

        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId, 0)).Count().ShouldBe(3);
    }
    
    [TestMethod]
    public async Task EventSourcesSunshineScenarioUsingEventStore()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await Sql.CreateAndInitializeEventStore();
        var eventSource = await eventStore.GetEventSource(functionId);

        async Task<object> FirstAsync() => await eventSource.All.FirstAsync();
        var task = FirstAsync();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventStore.AppendEvent(
            functionId,
            new StoredEvent("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        (await task).ShouldBe("hello world");
    }
    
    [TestMethod]
    public async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await Sql.CreateAndInitializeEventStore();
        var eventSource = await eventStore.GetEventSource(functionId);

        async Task<IList<object>> TakeTwo() => await eventSource.All.Take(2).ToList();
        var task = TakeTwo();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventStore.AppendEvent(
            functionId,
            new StoredEvent("hello world".ToJson(), typeof(string).SimpleQualifiedName(), "1")
        );
        await eventStore.AppendEvent(
            functionId,
            new StoredEvent("hello world".ToJson(), typeof(string).SimpleQualifiedName(), "1")
        );
        await eventStore.AppendEvent(
            functionId,
            new StoredEvent("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );

        var emits = await task;
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId, 0)).Count().ShouldBe(3);
    }
}