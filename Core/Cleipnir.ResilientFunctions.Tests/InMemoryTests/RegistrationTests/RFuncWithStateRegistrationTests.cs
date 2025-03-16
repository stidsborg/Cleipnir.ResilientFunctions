using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RFuncWithStateRegistrationTests
{
    private readonly FlowType _flowType = new FlowType("flowType");
    private const string flowInstance = "flowInstance";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rFunc = rFunctions
            .RegisterFunc<string, string>(
                _flowType,
                InnerFunc
            )
            .Invoke;

        var result = await rFunc(flowInstance, "hello world");
        result.ShouldBe("HELLO WORLD");
    }

    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        var serializer = new Serializer();
        using var rFunctions = new FunctionsRegistry(
            new InMemoryFunctionStore(),
            settings: new Settings(serializer: serializer)
        );
        
        var rFunc = rFunctions.RegisterFunc<string, string>(_flowType, InnerFunc).Invoke;

        var result = await rFunc(flowInstance, "hello world");
        result.ShouldBe("HELLO WORLD");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task<string> InnerFunc(string param)
    {
        await Task.CompletedTask;
        return param.ToUpper();
    }
    private FunctionsRegistry CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public byte[] Serialize<T>(T parameter)
        {
            Invoked = true;
            return Default.Serialize(parameter);
        }

        public byte[] Serialize(object? value, Type type) => Default.Serialize(value, type);

        public T Deserialize<T>(byte[] bytes)
            => Default.Deserialize<T>(bytes);
        
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