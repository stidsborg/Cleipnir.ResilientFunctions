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
        FuncRegistration<string, string> registration = null!;
        using var rFunctions = await CreateRFunctions(
            r =>
            {
                registration = r
                    .RegisterFunc<string, string>(
                        _flowType,
                        InnerFunc
                    );
            }
        );
        var rFunc = registration.Run;

        var result = await rFunc(flowInstance, "hello world");
        result.ShouldBe("HELLO WORLD");
    }

    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        var serializer = new Serializer();
        FuncRegistration<string, string> registration = null!;
        using var rFunctions = await FunctionsRegistry.CreateAndStart(
            new InMemoryFunctionStore(),
            settings: new Settings(serializer: serializer),
            setup: r => { registration = r.RegisterFunc<string, string>(_flowType, InnerFunc); }
        );
        
        var rFunc = registration.Run;

        var result = await rFunc(flowInstance, "hello world");
        result.ShouldBe("HELLO WORLD");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task<string> InnerFunc(string param)
    {
        await Task.CompletedTask;
        return param.ToUpper();
    }
    private Task<FunctionsRegistry> CreateRFunctions(Action<FunctionsRegistry> setup)
        => FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), setup);

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public byte[] Serialize(object value, Type type)
        {
            Invoked = true;
            return Default.Serialize(value, type);
        }

        public object Deserialize(byte[] bytes, Type type)
            => Default.Deserialize(bytes, type);
    }
}