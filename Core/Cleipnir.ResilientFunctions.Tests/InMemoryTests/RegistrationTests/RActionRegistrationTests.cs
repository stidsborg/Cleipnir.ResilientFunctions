using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RActionRegistrationTests
{
    private readonly FunctionTypeId _functionTypeId = new FunctionTypeId("FunctionTypeId");
    private const string FunctionInstanceId = "FunctionInstanceId";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rAction = rFunctions
            .RegisterAction<string>(_functionTypeId, InnerAction)
            .Invoke;

        await rAction(FunctionInstanceId, "hello world");
    }
    
    private Task InnerAction(string param) => Task.CompletedTask;
    private FunctionsRegistry CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;
        
        public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull
        {
            Invoked = true;
            return Default.SerializeParameter(parameter);
        }
        public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
            => Default.DeserializeParameter<TParam>(json, type);

        public StoredState SerializeState<TState>(TState state) where TState : WorkflowState
            => Default.SerializeState(state);
        public TState DeserializeState<TState>(string json, string type)
            where TState : WorkflowState
            => Default.DeserializeState<TState>(json, type);

        public StoredException SerializeException(Exception exception)
            => Default.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => Default.DeserializeException(storedException);
        
        public StoredResult SerializeResult<TResult>(TResult result) => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type) 
            => Default.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeMessage<TEvent>(TEvent @event) where TEvent : notnull
            => Default.SerializeMessage(@event);
        public object DeserializeMessage(string json, string type)
            => Default.DeserializeMessage(json, type);

        public string SerializeEffectResult<TResult>(TResult result)
            => Default.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(string json)
            => Default.DeserializeEffectResult<TResult>(json);
    }
}