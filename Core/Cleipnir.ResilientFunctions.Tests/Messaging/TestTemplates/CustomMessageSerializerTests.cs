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
        var functionId = TestFlowId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            param: Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var eventSerializer = new EventSerializer();
        var messagesWriter = new MessageWriter(functionId, functionStore, eventSerializer, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var registeredTimeouts = new RegisteredTimeouts(functionId, functionStore.TimeoutStore);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromSeconds(1),
            defaultMaxWait: TimeSpan.Zero,
            isWorkflowRunning: () => true,
            functionStore,
            eventSerializer,
            registeredTimeouts
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter);
        
        await messages.AppendMessage("hello world");
        
        eventSerializer.EventToSerialize.Count.ShouldBe(1);
        eventSerializer.EventToSerialize[0].ShouldBe("hello world");
        
        eventSerializer.EventToDeserialize.Count.ShouldBe(1);
        var (eventJson, eventType) = eventSerializer.EventToDeserialize[0];
        var deserializedEvent = DefaultSerializer.Instance.DeserializeMessage(eventJson.ToUtf8Bytes(), eventType.ToUtf8Bytes());
        deserializedEvent.ShouldBe("hello world");
    }

    private class EventSerializer : ISerializer
    {
        public Utils.SyncedList<object> EventToSerialize { get; } = new();
        public Utils.SyncedList<Tuple<string, string>> EventToDeserialize { get; }= new();

        public byte[] SerializeParameter<TParam>(TParam parameter)  
            => DefaultSerializer.Instance.SerializeParameter(parameter);

        public TParam DeserializeParameter<TParam>(byte[] json) 
            => DefaultSerializer.Instance.DeserializeParameter<TParam>(json);

        public StoredException SerializeException(Exception exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(storedException);

        public byte[] SerializeResult<TResult>(TResult result) 
            => DefaultSerializer.Instance.SerializeResult(result);
        public TResult DeserializeResult<TResult>(byte[] json) 
            => DefaultSerializer.Instance.DeserializeResult<TResult>(json);

        public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
        {
            EventToSerialize.Add(message);
            return DefaultSerializer.Instance.SerializeMessage(message);
        }
        public object DeserializeMessage(byte[] json, byte[] type)
        {
            EventToDeserialize.Add(Tuple.Create(json.ToStringFromUtf8Bytes(), type.ToStringFromUtf8Bytes()));
            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }

        public byte[] SerializeEffectResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(byte[] json)
            => DefaultSerializer.Instance.DeserializeEffectResult<TResult>(json);

        public byte[] SerializeState<TState>(TState state) where TState : FlowState, new()
            => DefaultSerializer.Instance.SerializeState(state);
        public TState DeserializeState<TState>(byte[] json) where TState : FlowState, new()
            => DefaultSerializer.Instance.DeserializeState<TState>(json);
    }
}