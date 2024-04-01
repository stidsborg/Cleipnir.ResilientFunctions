using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class EffectStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2",
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store
            .GetEffectResults(functionId)
            .ToListAsync()
            .SelectAsync(l => l.Any())
            .ShouldBeFalseAsync();
        
        await store.SetEffectResult(functionId, storedEffect1);
        
        var storedEffects = await store
            .GetEffectResults(functionId)
            .ToListAsync();
        storedEffects.Count.ShouldBe(1);
        var se = storedEffects[0];
        se.ShouldBe(storedEffect1);
        
        await store.SetEffectResult(functionId, storedEffect2);
        storedEffects = await store.GetEffectResults(functionId).ToListAsync();
        storedEffects.Count.ShouldBe(2);
        storedEffects[0].ShouldBe(storedEffect1);
        storedEffects[1].ShouldBe(storedEffect2);
        
        await store.SetEffectResult(functionId, storedEffect2);
        await store.GetEffectResults(functionId).ToListAsync();
        
        await store.SetEffectResult(functionId, storedEffect2);
        storedEffects = await store.GetEffectResults(functionId).ToListAsync();
        storedEffects.Count.ShouldBe(2);
        storedEffects[0].ShouldBe(storedEffect1);
        storedEffects[1].ShouldBe(storedEffect2);
    }
    
    public abstract Task SingleEffectWithResultLifeCycle();
    protected async Task SingleEffectWithResultLifeCycle(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var effect = new StoredEffect(
            "EffectId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetEffectResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();
        
        await store.SetEffectResult(functionId, effect);
        var storedEffect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        storedEffect.ShouldBe(effect);

        effect = effect with { WorkStatus = WorkStatus.Completed, Result = "Hello World" };
        await store.SetEffectResult(functionId, effect);
        storedEffect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        storedEffect.ShouldBe(effect);
    }
    
    public abstract Task SingleFailingEffectLifeCycle();
    protected async Task SingleFailingEffectLifeCycle(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var storedException = new StoredException(
            "Some Exception Message",
            "SomeStackTrace",
            "Some Exception Type"
        );
        var storedEffect = new StoredEffect(
            "EffectId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetEffectResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();
        
        await store.SetEffectResult(functionId, storedEffect);
        var effect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        effect.ShouldBe(storedEffect);

        storedEffect = storedEffect with { WorkStatus = WorkStatus.Completed, StoredException = storedException };
        await store.SetEffectResult(functionId, storedEffect);
        effect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        effect.ShouldBe(storedEffect);
    }
    
    public abstract Task EffectCanBeDeleted();
    protected async Task EffectCanBeDeleted(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2",
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store.SetEffectResult(functionId, storedEffect1);
        await store.SetEffectResult(functionId, storedEffect2);

        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Count() == 2)
            .ShouldBeTrueAsync();

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId);
        var storedEffects = await store.GetEffectResults(functionId).ToListAsync();
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId);
        storedEffects = await store.GetEffectResults(functionId).ToListAsync();
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);
        
        await store.DeleteEffectResult(functionId, storedEffect1.EffectId);
        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Any())
            .ShouldBeFalseAsync();
    }
    
    public abstract Task DeleteFunctionIdDeletesAllRelatedEffects();
    protected async Task DeleteFunctionIdDeletesAllRelatedEffects(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var otherFunctionId = new FunctionId(functionId.TypeId, functionInstanceId: functionId.InstanceId + "123");
        
        var storedEffect1 = new StoredEffect(
            "EffectId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2",
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        
        await store.SetEffectResult(functionId, storedEffect1);
        await store.SetEffectResult(functionId, storedEffect2);
        await store.SetEffectResult(otherFunctionId, storedEffect1);

        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Count() == 2)
            .ShouldBeTrueAsync();

        await store.Remove(functionId);

        await store
            .GetEffectResults(functionId)
            .SelectAsync(e => e.Any())
            .ShouldBeFalseAsync();
        
        await store
            .GetEffectResults(otherFunctionId)
            .SelectAsync(e => e.Any())
            .ShouldBeTrueAsync();
    }
}