using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class CustomMessageSerializerTests
{
    public abstract Task CustomEventSerializerIsUsedWhenSpecified();
    protected async Task CustomEventSerializerIsUsedWhenSpecified(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var eventSerializer = new EventSerializer();
        var messagesWriter = new MessageWriter(functionId, functionStore, eventSerializer, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromSeconds(1),
            isWorkflowRunning: () => true,
            functionStore,
            eventSerializer,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);
        
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

        public StoredException SerializeException(Exception exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => DefaultSerializer.Instance.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
        {
            EventToSerialize.Add(message);
            return DefaultSerializer.Instance.SerializeMessage(message);
        }
        public object DeserializeMessage(string json, string type)
        {
            EventToDeserialize.Add(Tuple.Create(json, type));
            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }

        public string SerializeEffectResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(string json)
            => DefaultSerializer.Instance.DeserializeEffectResult<TResult>(json);

        public string SerializeState<TState>(TState state) where TState : WorkflowState, new()
            => DefaultSerializer.Instance.SerializeState(state);
        public TState DeserializeState<TState>(string json) where TState : WorkflowState, new()
            => DefaultSerializer.Instance.DeserializeState<TState>(json);
    }
}