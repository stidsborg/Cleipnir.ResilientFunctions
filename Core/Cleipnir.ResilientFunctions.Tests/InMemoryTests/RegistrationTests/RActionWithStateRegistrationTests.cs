using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RActionWithStateRegistrationTests
{
    private readonly FlowType _flowType = new FlowType("flowType");
    private const string flowInstance = "flowInstance";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
        var rAction = rFunctions
            .RegisterAction<string>(
                _flowType,
                InnerAction
            )
            .Invoke;

        await rAction(flowInstance, "hello world");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        var serializer = new Serializer();
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore(), new Settings(serializer: serializer));
        var rAction = rFunctions.RegisterAction<string>(_flowType, InnerAction).Invoke;

        await rAction(flowInstance, "hello world");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task InnerAction(string param) => await Task.CompletedTask;
    
    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public byte[] Serialize(object? value, Type type)
        {
            Invoked = true;
            return Default.Serialize(value, type);
        }

        public object Deserialize(byte[] json, Type type)
            => Default.Deserialize(json, type);

        public StoredException SerializeException(FatalWorkflowException exception)
            => Default.SerializeException(exception);
        public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
            => Default.DeserializeException(flowId, storedException);

        public SerializedMessage SerializeMessage(object message, Type messageType)
            => Default.SerializeMessage(message, messageType);
        public object DeserializeMessage(byte[] json, byte[] type)
            => Default.DeserializeMessage(json, type);
    }
}