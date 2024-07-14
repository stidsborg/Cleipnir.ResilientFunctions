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
    private readonly FlowType _flowType = new FlowType("FunctionTypeId");
    private const string FunctionInstanceId = "FunctionInstanceId";
    
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

        await rAction(FunctionInstanceId, "hello world");
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

        await rAction(FunctionInstanceId, "hello world");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task InnerAction(string param) => await Task.CompletedTask;
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