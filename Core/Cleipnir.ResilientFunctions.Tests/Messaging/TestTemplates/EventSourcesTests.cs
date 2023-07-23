using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class EventSourcesTests
{
    public abstract Task EventSourcesSunshineScenario();
    protected async Task EventSourcesSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();
        
        var task = eventSource.Next();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.AppendEvent("hello world");

        (await task).ShouldBe("hello world");
    }
    
    public abstract Task ExistingEventsShouldBeSameAsAllAfterEmit();
    protected async Task ExistingEventsShouldBeSameAsAllAfterEmit(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();

        await eventSource.AppendEvent("hello world");

        var nextEvent = await eventSource.Next(maxWaitMs: 1_000);
        nextEvent.ShouldBe("hello world");

        var success = eventSource.OfType<string>().TryNext(out var next);
        success.ShouldBeTrue();
        next.ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();
        
        var task = eventSource.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.AppendEvent("hello world", idempotencyKey: "1");
        await eventSource.AppendEvent("hello world", idempotencyKey: "1");
        await eventSource.AppendEvent("hello universe");

        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.EventStore.GetEvents(functionId)).Count().ShouldBe(2);
    }
    
    public abstract Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully();
    protected async Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();

        var task = eventSource.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        await eventSource.AppendEvents(new EventAndIdempotencyKey[]
        {
            new("hello world", "1"),
            new("hello world", "1"),
            new("hello universe")
        });

        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.EventStore.GetEvents(functionId)).Count().ShouldBe(2);
    }

    public abstract Task EventSourcesSunshineScenarioUsingEventStore();
    protected async Task EventSourcesSunshineScenarioUsingEventStore(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();
        
        var task = eventSource.Next();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await functionStore.EventStore.AppendEvent(
            functionId,
            new StoredEvent(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName())
        );

        (await task).ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            DefaultSerializer.Instance
        );
        await eventSource.Initialize();

        var task = eventSource.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        var eventStore = functionStore.EventStore;
        await eventStore.AppendEvent(
            functionId,
            new StoredEvent(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName(), "1")
        );
        await eventStore.AppendEvent(
            functionId,
            new StoredEvent(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName(), "1")
        );
        await eventStore.AppendEvent(
            functionId,
            new StoredEvent(JsonExtensions.ToJson("hello universe"), typeof(string).SimpleQualifiedName())
        );

        await task;
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await eventStore.GetEvents(functionId)).Count().ShouldBe(2);
    }
    
    public abstract Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations();
    protected async Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredScrapbook, 
            leaseExpiration: DateTime.UtcNow.Ticks
        );
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore.EventStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        using var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            new EventSourceWriter(
                functionId, functionStore.EventStore, 
                new ExceptionThrowingEventSerializer(typeof(int)), 
                scheduleReInvocation: (_, _, _) => Task.CompletedTask
            ),
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            new ExceptionThrowingEventSerializer(typeof(int))
        );
        await eventSource.Initialize();
        
        await eventSource.AppendEvent("hello world");
        await Should.ThrowAsync<EventProcessingException>(eventSource.AppendEvent(1));
        await Should.ThrowAsync<EventProcessingException>(async () => await eventSource.Skip(1).Next());
        Should.Throw<EventProcessingException>(() => eventSource.ToList());
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