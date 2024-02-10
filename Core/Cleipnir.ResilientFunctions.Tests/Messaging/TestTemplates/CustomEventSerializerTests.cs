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
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredState, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var eventSerializer = new EventSerializer();
        var messagesWriter = new MessageWriter(functionId, functionStore, eventSerializer, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messages = new Messages(
            functionId,
            functionStore.MessageStore,
            messagesWriter,
            new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1)),
            pullFrequency: null,
            eventSerializer
        );
        
        await messages.AppendMessage("hello world");
        
        eventSerializer.EventToSerialize.Count.ShouldBe(1);
        eventSerializer.EventToSerialize[0].ShouldBe("hello world");
        
        eventSerializer.EventToDeserialize.Count.ShouldBe(1);
        var (eventJson, eventType) = eventSerializer.EventToDeserialize[0];
        var deserializedEvent = DefaultSerializer.Instance.DeserializeMessage(eventJson, eventType);
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

        public StoredState SerializeState<TState>(TState state) where TState : WorkflowState
            => DefaultSerializer.Instance.SerializeState(state);
        public TState DeserializeState<TState>(string json, string type) where TState : WorkflowState
            => DefaultSerializer.Instance.DeserializeState<TState>(json, type);

        public StoredException SerializeException(Exception exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => DefaultSerializer.Instance.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeMessage<TEvent>(TEvent @event) where TEvent : notnull
        {
            EventToSerialize.Add(@event);
            return DefaultSerializer.Instance.SerializeMessage(@event);
        }
        public object DeserializeMessage(string json, string type)
        {
            EventToDeserialize.Add(Tuple.Create(json, type));
            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }

        public string SerializeActivityResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeActivityResult(result);
        public TResult DeserializeActivityResult<TResult>(string json)
            => DefaultSerializer.Instance.DeserializeActivityResult<TResult>(json);
    }
}