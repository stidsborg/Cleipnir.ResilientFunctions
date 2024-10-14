using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StatesStoreTests
{
    public abstract Task SunshineScenario();
    public async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var statesStore = await storeTask.SelectAsync(s => s.EffectsStore);
        var flowId = TestFlowId.Create();

        var initialStates = await statesStore.GetEffectResults(flowId);
        initialStates.ShouldBeEmpty();

        await statesStore.SetEffectResult(
            flowId,
            StoredEffect.CreateState(new StoredState("Id#1", "SomeJson#1"))
        );
        await statesStore.SetEffectResult(
            flowId,
            StoredEffect.CreateState(new StoredState("Id#2", "SomeJson#2"))
        );

        var states = await statesStore.GetEffectResults(flowId);
        states.Count.ShouldBe(2);

        var state1 = states.Single(s => s.EffectId == "Id#1");
        state1.IsState.ShouldBeTrue();
        state1.Result.ShouldBe("SomeJson#1");

        var state2 = states.Single(s => s.EffectId == "Id#2");
        state2.IsState.ShouldBeTrue();
        state2.Result.ShouldBe("SomeJson#2");

        await statesStore.DeleteEffectResult(flowId, state1.EffectId, isState: true);
        
        states = await statesStore.GetEffectResults(flowId);
        states.Count.ShouldBe(1);
        state2 = states.Single(s => s.EffectId == "Id#2");
        state2.Result.ShouldBe("SomeJson#2");
    }
}