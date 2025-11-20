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
                await workflow.Effect.CreateOrGet("0", toReturn);
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
            var storedId = registration.MapToStoredId(functionId.Instance);
            var storedFunction = await store.GetFunction(storedId);
            storedFunction.ShouldNotBeNull();
            var results = await store.GetResults([storedId]);
            var resultBytes = results[storedId];
            resultBytes.ShouldNotBeNull();
            var storedResult = resultBytes.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
            storedResult.ShouldBe("HELLO");
            var effects = await store.EffectsStore.GetEffectResults(registration.MapToStoredId(functionId.Instance));
            effects
                .Single(e => e.EffectId == "0".ToEffectId())
                .Result!
                .ToStringFromUtf8Bytes()
                .DeserializeFromJsonTo<string>()
                .ShouldBe("HELLO");
            
            unhandledExceptionHandler.ShouldNotHaveExceptions();
        }
    }
}