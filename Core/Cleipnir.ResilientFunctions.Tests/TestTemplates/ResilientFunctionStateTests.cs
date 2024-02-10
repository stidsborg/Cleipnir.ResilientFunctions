using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class ResilientFunctionStateTests
    {
        private readonly DefaultSerializer _serializer = DefaultSerializer.Instance;
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenario).ToFunctionTypeId();
            async Task<string> ToUpper(string s, WorkflowState state)
            {
                var toReturn = s.ToUpper();
                state.Scrap = toReturn;
                await state.Save();
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

            var rFunc = functionsRegistry
                .RegisterFunc(
                    functionTypeId,
                    (string s, WorkflowState state) => ToUpper(s, state)
                ).Invoke;

            var result = await rFunc("hello", "hello");
            result.ShouldBe("HELLO");
            
            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.Deserialize<string>(_serializer);
            storedResult.ShouldBe("HELLO");
            storedFunction.State.ShouldNotBeNull();
            var state = storedFunction.State.Deserialize<WorkflowState>(_serializer);
            state.Scrap.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        private class WorkflowState : Domain.WorkflowState
        {
            public string Scrap { get; set; } = "";
        }
    }
}