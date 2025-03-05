using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class ResilientFunctionStateTests
    {
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            var flowType = nameof(SunshineScenario).ToFlowType();
            async Task<string> ToUpper(string s, Workflow workflow)
            {
                var toReturn = s.ToUpper();
                await workflow.Effect.CreateOrGet("Scrap", toReturn);
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

            var registration = functionsRegistry
                .RegisterFunc<string, string>(
                    flowType,
                    ToUpper
                );
            var rFunc = registration.Invoke;

            var result = await rFunc("hello", "hello");
            result.ShouldBe("HELLO");

            var functionId = new FlowId(flowType, "hello".ToFlowInstance());
            var storedFunction = await store.GetFunction(registration.MapToStoredId(functionId.Instance));
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
            storedResult.ShouldBe("HELLO");
            var effects = await store.EffectsStore.GetEffectResults(registration.MapToStoredId(functionId.Instance));
            effects
                .Single(e => e.EffectId == "Scrap".ToEffectId())
                .Result!
                .ToStringFromUtf8Bytes()
                .DeserializeFromJsonTo<string>()
                .ShouldBe("HELLO");
            
            unhandledExceptionHandler.ShouldNotHaveExceptions();
        }

        private class FlowState : Domain.FlowState
        {
            public string Scrap { get; set; } = "";
        }
    }
}