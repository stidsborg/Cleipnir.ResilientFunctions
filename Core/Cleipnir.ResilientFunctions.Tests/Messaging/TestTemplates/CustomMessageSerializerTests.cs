using System;
using System.Collections.Generic;
using System.Linq;
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
        var functionStore = await functionStoreTask;
        var registry = new FunctionsRegistry(
            functionStore,
            new Settings(serializer: new EventSerializer())
        );

        var registration = registry.RegisterParamless(
            flowId.Type,
            inner: workflow => workflow.Message<string>()
        );

        var scheduled = await registration.Schedule(flowId.Instance);
        await registration.MessageWriters.For(flowId.Instance).AppendMessage("hello world");

        await BusyWait.Until(() => EventSerializer.EventToDeserialize.Count > 0);
        EventSerializer.EventToDeserialize.First().Item1.DeserializeFromJsonTo<string>().ShouldBe("hello world");
        EventSerializer.EventToSerialize.First().ShouldBe("hello world");
        await scheduled.Completion();
    }

    private class EventSerializer : ISerializer
    {
        public static Utils.SyncedList<object> EventToSerialize { get; } = new();
        public static Utils.SyncedList<Tuple<string, string>> EventToDeserialize { get; }= new();

        public byte[] Serialize(object value, Type type)
        {
            EventToSerialize.Add(value);
            return DefaultSerializer.Instance.Serialize(value, type);
        }

        public object Deserialize(byte[] json, Type type)
        {
            EventToDeserialize.Add(Tuple.Create(json.ToStringFromUtf8Bytes(), type.FullName!));
            return DefaultSerializer.Instance.Deserialize(json, type);
        }

    }
}