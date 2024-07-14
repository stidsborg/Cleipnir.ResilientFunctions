using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RFuncRegistrationTests
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
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rFunc = rFunctions
            .RegisterFunc<string, string>(
                _flowType,
                InnerFunc,
                new Settings(serializer: serializer)
            )
            .Invoke;

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
        
        public string SerializeParameter<TParam>(TParam parameter) 
        {
            Invoked = true;
            return Default.SerializeParameter(parameter);
        }
        public TParam DeserializeParameter<TParam>(string json) 
            => Default.DeserializeParameter<TParam>(json);

        public StoredException SerializeException(Exception exception)
            => Default.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => Default.DeserializeException(storedException);

        public string SerializeResult<TResult>(TResult result)
            => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json) 
            => Default.DeserializeResult<TResult>(json);

        public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
            => Default.SerializeMessage(message);
        public object DeserializeMessage(string json, string type)
            => Default.DeserializeMessage(json, type);

        public string SerializeEffectResult<TResult>(TResult result)
            => Default.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(string json)
            => Default.DeserializeEffectResult<TResult>(json);

        public string SerializeState<TState>(TState state) where TState : FlowState, new()
            => Default.SerializeState(state);
        
        public TState DeserializeState<TState>(string json) where TState : FlowState, new()
            => Default.DeserializeState<TState>(json);
    }
}