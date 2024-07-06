using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
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
            async Task<string> ToUpper(string s, Workflow workflow)
            {
                var toReturn = s.ToUpper();
                await workflow.Effect.CreateOrGet("Scrap", toReturn);
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

            var rFunc = functionsRegistry
                .RegisterFunc<string, string>(
                    functionTypeId,
                    ToUpper
                ).Invoke;

            var result = await rFunc("hello", "hello");
            result.ShouldBe("HELLO");

            var functionId = new FunctionId(functionTypeId, "hello".ToFunctionInstanceId());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.DeserializeFromJsonTo<string>();
            storedResult.ShouldBe("HELLO");
            var effects = await store.EffectsStore.GetEffectResults(functionId);
            effects
                .Single(e => e.EffectId == "Scrap")
                .Result!
                .DeserializeFromJsonTo<string>()
                .ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        private class FlowState : Domain.FlowState
        {
            public string Scrap { get; set; } = "";
        }
    }
}