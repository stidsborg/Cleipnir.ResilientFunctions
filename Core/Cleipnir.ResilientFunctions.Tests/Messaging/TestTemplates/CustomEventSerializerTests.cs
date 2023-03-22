using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class CustomEventSerializerTests
{
    public abstract Task CustomEventSerializerIsUsedWhenSpecified();
    protected async Task CustomEventSerializerIsUsedWhenSpecified(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        var eventSerializer = new EventSerializer();
        var eventSourceWriter = new EventSourceWriter(functionId, functionStore, eventSerializer, scheduleReInvocation: (_, _, _) => Task.CompletedTask);
        var eventSource = new EventSource(
            functionId,
            functionStore.EventStore,
            eventSourceWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, eventSourceWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            eventSerializer
        );
        await eventSource.Initialize();
        
        await eventSource.AppendEvent("hello world");
        
        eventSerializer.EventToSerialize.Count.ShouldBe(1);
        eventSerializer.EventToSerialize[0].ShouldBe("hello world");
        
        eventSerializer.EventToDeserialize.Count.ShouldBe(1);
        var (eventJson, eventType) = eventSerializer.EventToDeserialize[0];
        var deserializedEvent = DefaultSerializer.Instance.DeserializeEvent(eventJson, eventType);
        deserializedEvent.ShouldBe("hello world");
    }

    private class EventSerializer : ISerializer
    {
        public Utils.SyncedList<object> EventToSerialize { get; } = new();
        public Utils.SyncedList<Tuple<string, string>> EventToDeserialize { get; }= new();

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
        {
            EventToSerialize.Add(@event);
            return DefaultSerializer.Instance.SerializeEvent(@event);
        }
        public object DeserializeEvent(string json, string type)
        {
            EventToDeserialize.Add(Tuple.Create(json, type));
            return DefaultSerializer.Instance.DeserializeEvent(json, type);
        }
    }
}