using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
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
        var flowId = TestStoredId.Create();

        var initialStates = await statesStore.GetEffectResults(flowId);
        initialStates.ShouldBeEmpty();

        await statesStore.SetEffectResult(
            flowId,
            StoredEffect.CreateState(new StoredState("Id#1", "SomeJson#1".ToUtf8Bytes())),
            session: null
        );
        await statesStore.SetEffectResult(
            flowId,
            StoredEffect.CreateState(new StoredState("Id#2", "SomeJson#2".ToUtf8Bytes())),
            session: null
        );

        var states = await statesStore.GetEffectResults(flowId);
        states.Count.ShouldBe(2);

        var state1 = states.Single(s => s.EffectId == "Id#1".ToEffectId(EffectType.State));
        state1.Result.ShouldBe("SomeJson#1".ToUtf8Bytes());

        var state2 = states.Single(s => s.EffectId == "Id#2".ToEffectId(EffectType.State));
        state2.Result.ShouldBe("SomeJson#2".ToUtf8Bytes());

        await statesStore.DeleteEffectResult(flowId, state1.EffectId, storageSession: null);
        
        states = await statesStore.GetEffectResults(flowId);
        states.Count.ShouldBe(1);
        state2 = states.Single(s => s.EffectId == "Id#2".ToEffectId(EffectType.State));
        state2.Result.ShouldBe("SomeJson#2".ToUtf8Bytes());
    }
}