using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var eventSerializer = new EventSerializer();
        var messagesWriter = new MessageWriter(storedId, functionStore, eventSerializer);
        var effectResults = new EffectResults(flowId, storedId, new List<StoredEffect>(), functionStore.EffectsStore, DefaultSerializer.Instance, storageSession: null);
        var minimumTimeout = new FlowMinimumTimeout();
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, minimumTimeout);
        var registeredTimeouts = new FlowRegisteredTimeouts(
            effect, 
            utcNow: () => DateTime.UtcNow, 
            minimumTimeout, 
            publishTimeoutEvent: t => messagesWriter.AppendMessage(t),
            unhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            flowId);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromSeconds(1),
            defaultMaxWait: TimeSpan.Zero,
            isWorkflowRunning: () => true,
            functionStore,
            eventSerializer,
            registeredTimeouts,
            initialMessages: null,
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
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

        public byte[] Serialize<T>(T value)  
            => DefaultSerializer.Instance.Serialize(value);

        public byte[] Serialize(object? value, Type type) => DefaultSerializer.Instance.Serialize(value, type);

        public T Deserialize<T>(byte[] json) 
            => DefaultSerializer.Instance.Deserialize<T>(json);

        public StoredException SerializeException(FatalWorkflowException exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(flowId, storedException);

        public SerializedMessage SerializeMessage(object message, Type messageType)
        {
            EventToSerialize.Add(message);
            return DefaultSerializer.Instance.SerializeMessage(message, messageType);
        }
        public object DeserializeMessage(byte[] json, byte[] type)
        {
            EventToDeserialize.Add(Tuple.Create(json.ToStringFromUtf8Bytes(), type.ToStringFromUtf8Bytes()));
            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }
    }
}