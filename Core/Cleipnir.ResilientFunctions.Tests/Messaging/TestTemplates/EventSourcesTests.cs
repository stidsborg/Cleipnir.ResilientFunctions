using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventSourcesTests
{
    public abstract Task EventSourcesSunshineScenario();
    protected async Task EventSourcesSunshineScenario(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );

        // ReSharper disable once AccessToDisposedClosure
        async Task<object> FirstAsync() => await eventSource.All.FirstAsync();
        var task = FirstAsync();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.Append("hello world");

        (await task).ShouldBe("hello world");
    }
    
    public abstract Task ExistingEventsShouldBeSameAsAllAfterEmit();
    protected async Task ExistingEventsShouldBeSameAsAllAfterEmit(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );

        await eventSource.Append("hello world");

        var nextEvent = await eventSource.All.NextEvent(maxWaitMs: 1_000);
        nextEvent.ShouldBe("hello world");
        eventSource.Existing.Count.ShouldBe(1);
        eventSource.Existing[0].ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IEventStore> eventStoreTask)
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await eventStoreTask;
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );

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
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );

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
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();

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
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, DefaultSerializer.Instance),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();

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
        using var eventSource = new EventSource(
            functionId,
            eventStore,
            new EventSourceWriter(functionId, eventStore, new ExceptionThrowingEventSerializer(typeof(int))),
            pullFrequency: null,
            new ExceptionThrowingEventSerializer(typeof(int))
        );

        await eventSource.Append("hello world");
        await Should.ThrowAsync<EventProcessingException>(eventSource.Append(1));
        await Should.ThrowAsync<EventProcessingException>(async () => await eventSource.All.Skip(1).NextEvent());
        Should.Throw<EventProcessingException>(() => eventSource.Existing.ToList());
    }
    
    private class ExceptionThrowingEventSerializer : ISerializer
    {
        private readonly Type _failDeserializationOnType;

        public ExceptionThrowingEventSerializer(Type failDeserializationOnType) 
            => _failDeserializationOnType = failDeserializationOnType;

        public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull 
            => DefaultSerializer.Instance.SerializeParameter(parameter);

        public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
            => DefaultSerializer.Instance.DeserializeParameter<TParam>(json, type);

        public StoredScrapbook SerializeScrapbook<TScrapbook>(TScrapbook scrapbook) where TScrapbook : RScrapbook
            => DefaultSerializer.Instance.SerializeScrapbook(scrapbook);
        public TScrapbook DeserializeScrapbook<TScrapbook>(string json, string type) where TScrapbook : RScrapbook
            => DefaultSerializer.Instance.DeserializeScrapbook<TScrapbook>(json, type);

        public StoredException SerializeException(Exception exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => DefaultSerializer.Instance.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeEvent<TEvent>(TEvent @event) where TEvent : notnull
            => DefaultSerializer.Instance.SerializeEvent(@event);

        public object DeserializeEvent(string json, string type)
        {
            var eventType = Type.GetType(type)!;
            if (eventType == _failDeserializationOnType)
                throw new Exception("Deserialization exception");

            return DefaultSerializer.Instance.DeserializeEvent(json, type);
        }
    }
}