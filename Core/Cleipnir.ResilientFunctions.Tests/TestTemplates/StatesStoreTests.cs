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
        var statesStore = await storeTask.SelectAsync(s => s.StatesStore);
        var functionId = TestFunctionId.Create();

        var initialStates = await statesStore.GetStates(functionId);
        initialStates.ShouldBeEmpty();

        await statesStore.UpsertState(functionId, new StoredState("Id#1", "SomeJson#1"));
        await statesStore.UpsertState(functionId, new StoredState("Id#2", "SomeJson#2"));

        var states = await statesStore.GetStates(functionId).ToListAsync();
        states.Count.ShouldBe(2);

        var state1 = states.Single(s => s.StateId == "Id#1");
        state1.StateJson.ShouldBe("SomeJson#1");

        var state2 = states.Single(s => s.StateId == "Id#2");
        state2.StateJson.ShouldBe("SomeJson#2");

        await statesStore.RemoveState(functionId, state1.StateId);
        
        states = await statesStore.GetStates(functionId).ToListAsync();
        states.Count.ShouldBe(1);
        state2 = states.Single(s => s.StateId == "Id#2");
        state2.StateJson.ShouldBe("SomeJson#2");
    }
}