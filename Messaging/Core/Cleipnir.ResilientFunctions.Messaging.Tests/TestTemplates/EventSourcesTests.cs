using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.TestTemplates;

public abstract class EventSourcesTests
{
    public abstract Task EventSourcesSunshineScenario();    
    public async Task EventSourcesSunshineScenario(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        var eventSources = new EventSources(eventStore);
        using var eventSource = await eventSources.Get(functionId);

        // ReSharper disable once AccessToDisposedClosure
        async Task<object> FirstAsync() => await eventSource.All.FirstAsync();
        var task = FirstAsync();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.Emit("hello world");

        (await task).ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    public async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        var eventSources = new EventSources(eventStore);
        using var eventSource = await eventSources.Get(functionId);

        // ReSharper disable once AccessToDisposedClosure
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

    public abstract Task EventSourcesSunshineScenarioUsingEventStore();    
    public async Task EventSourcesSunshineScenarioUsingEventStore(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        var eventSources = new EventSources(eventStore);
        using var eventSource = await eventSources.Get(functionId);

        // ReSharper disable once AccessToDisposedClosure
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

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore();
    public async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        var eventSources = new EventSources(eventStore);
        using var eventSource = await eventSources.Get(functionId);

        // ReSharper disable once AccessToDisposedClosure
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

        await task;
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId, 0)).Count().ShouldBe(3);
    }
}