using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
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
        using var rFunctions = CreateRFunctions();
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
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rAction = rFunctions
            .RegisterAction<string>(
                _flowType,
                InnerAction,
                new Settings(serializer: serializer)
            )
            .Invoke;

        await rAction(flowInstance, "hello world");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task InnerAction(string param) => await Task.CompletedTask;
    private FunctionsRegistry CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public byte[] SerializeParameter<TParam>(TParam parameter) 
        {
            Invoked = true;
            return Default.SerializeParameter(parameter);
        }

        public TParam DeserializeParameter<TParam>(byte[] json) 
            => Default.DeserializeParameter<TParam>(json);

        public StoredException SerializeException(Exception exception)
            => Default.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => Default.DeserializeException(storedException);

        public byte[] SerializeResult<TResult>(TResult result)  
            => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(byte[] json) 
            => Default.DeserializeResult<TResult>(json);

        public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
            => Default.SerializeMessage(message);
        public object DeserializeMessage(byte[] json, byte[] type)
            => Default.DeserializeMessage(json, type);

        public byte[] SerializeEffectResult<TResult>(TResult result)
            => Default.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(byte[] json)
            => Default.DeserializeEffectResult<TResult>(json);

        public byte[] SerializeState<TState>(TState state) where TState : FlowState, new()
            => Default.SerializeState(state);
        public TState DeserializeState<TState>(byte[] json) where TState : FlowState, new()
            => Default.DeserializeState<TState>(json);
    }
}