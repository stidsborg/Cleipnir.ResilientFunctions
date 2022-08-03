using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;
using Cleipnir.ResilientFunctions.Messaging.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.TestTemplates;

public abstract class EventSourcesTests
{
    public abstract Task EventSourcesSunshineScenario();
    protected async Task EventSourcesSunshineScenario(Task<IEventStore> eventStoreTask)
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

        await eventSource.Append("hello world");

        (await task).ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IEventStore> eventStoreTask)
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

        await eventSource.Append("hello world", "1");
        await eventSource.Append("hello world", "1");
        await eventSource.Append("hello universe");

        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId, 0)).Count().ShouldBe(3);
    }
    
    public abstract Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully();
    protected async Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(Task<IEventStore> eventStoreTask)
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
        await eventSource.Append(new EventAndIdempotencyKey[]
        {
            new("hello world", "1"),
            new("hello world", "1"),
            new("hello universe")
        });

        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId, 0)).Count().ShouldBe(3);
    }

    public abstract Task EventSourcesSunshineScenarioUsingEventStore();
    protected async Task EventSourcesSunshineScenarioUsingEventStore(Task<IEventStore> eventStoreTask)
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
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(Task<IEventStore> eventStoreTask)
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
    
    public abstract Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations();
    protected async Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        var eventSources = new EventSources(eventStore, eventSerializer: new ExceptionThrowingEventSerializer(typeof(int)));
        using var eventSource = await eventSources.Get(functionId);

        await eventSource.Append("hello world");
        await Should.ThrowAsync<EventProcessingException>(eventSource.Append(1));
        await Should.ThrowAsync<EventProcessingException>(async () => await eventSource.All.Skip(1).NextEvent());
        Should.Throw<EventProcessingException>(() => eventSource.Existing.ToList());
    }
    
    private class ExceptionThrowingEventSerializer : IEventSerializer
    {
        private readonly Type _failDeserializationOnType;

        public ExceptionThrowingEventSerializer(Type failDeserializationOnType) 
            => _failDeserializationOnType = failDeserializationOnType;

        public string SerializeEvent(object @event) 
            => DefaultEventSerializer.Instance.SerializeEvent(@event);

        public object DeserializeEvent(string json, string type)
        {
            var eventType = Type.GetType(type)!;
            if (eventType == _failDeserializationOnType)
                throw new Exception("Deserialization exception");

            return DefaultEventSerializer.Instance.DeserializeEvent(json, type);
        }
    }
}