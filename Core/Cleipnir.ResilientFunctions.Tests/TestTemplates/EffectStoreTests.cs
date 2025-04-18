﻿using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Storage.CrudOperation;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class EffectStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store
            .GetEffectResults(functionId)
            .SelectAsync(l => l.Any())
            .ShouldBeFalseAsync();
        
        await store.SetEffectResult(functionId, storedEffect1);
        
        var storedEffects = await store
            .GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        var se = storedEffects[0];
        se.ShouldBe(storedEffect1);
        
        await store.SetEffectResult(functionId, storedEffect2);
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Any(s => s == storedEffect1).ShouldBeTrue();
        storedEffects.Any(s => s == storedEffect2).ShouldBeTrue();
        
        await store.SetEffectResult(functionId, storedEffect2);
        await store.GetEffectResults(functionId);
        
        await store.SetEffectResult(functionId, storedEffect2);
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Any(s => s == storedEffect1).ShouldBeTrue();
        storedEffects.Any(s => s == storedEffect2).ShouldBeTrue();
    }
    
    public abstract Task SingleEffectWithResultLifeCycle();
    protected async Task SingleEffectWithResultLifeCycle(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var effect = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
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

        effect = effect with { WorkStatus = WorkStatus.Completed, Result = "Hello World".ToUtf8Bytes() };
        await store.SetEffectResult(functionId, effect);
        storedEffect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        
        storedEffect.EffectId.ShouldBe(effect.EffectId);
        storedEffect.StoredException.ShouldBe(effect.StoredException);
        storedEffect.Result!.ToStringFromUtf8Bytes().ShouldBe(effect.Result.ToStringFromUtf8Bytes());
        storedEffect.WorkStatus.ShouldBe(effect.WorkStatus);
        
    }
    
    public abstract Task SingleFailingEffectLifeCycle();
    protected async Task SingleFailingEffectLifeCycle(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var storedException = new StoredException(
            "Some Exception Message",
            "SomeStackTrace",
            "Some Exception Type"
        );
        var storedEffect = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
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
        var functionId = TestStoredId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
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

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId.ToStoredEffectId());
        var storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId.ToStoredEffectId());
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);
        
        await store.DeleteEffectResult(functionId, storedEffect1.EffectId.ToStoredEffectId());
        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Any())
            .ShouldBeFalseAsync();
    }
    
    public abstract Task DeleteFunctionIdDeletesAllRelatedEffects();
    protected async Task DeleteFunctionIdDeletesAllRelatedEffects(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var otherFunctionId = new StoredId((functionId.Type.Value + 1).ToStoredType(), Instance: (functionId.Instance + "123").ToStoredInstance());
        
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
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
    
    public abstract Task TruncateDeletesAllEffects();
    protected async Task TruncateDeletesAllEffects(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var otherFunctionId = new StoredId(functionId.Type, Instance: (functionId.Instance + "123").ToStoredInstance());
        
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        
        await store.SetEffectResult(functionId, storedEffect1);
        await store.SetEffectResult(functionId, storedEffect2);
        await store.SetEffectResult(otherFunctionId, storedEffect1);

        await store.Truncate();
        
        await store
            .GetEffectResults(functionId)
            .SelectAsync(e => e.Any())
            .ShouldBeFalseAsync();
        
        await store
            .GetEffectResults(otherFunctionId)
            .SelectAsync(e => e.Any())
            .ShouldBeFalseAsync();
    }
    
    public abstract Task BulkInsertTest();
    protected async Task BulkInsertTest(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );

        await store.SetEffectResults(
            storedId,
            [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)]
        );

        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(2);
        var effect1 = effects.Single(e => e.EffectId == storedEffect1.EffectId);
        effect1.Result.ShouldBe("some result 1".ToUtf8Bytes());
        var effect2 = effects.Single(e => e.EffectId == storedEffect2.EffectId);
        effect2.Result.ShouldBe("some result 2".ToUtf8Bytes());
    }
    
    public abstract Task BulkInsertAndDeleteTest();
    protected async Task BulkInsertAndDeleteTest(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect3 = new StoredEffect(
            "EffectId3".ToEffectId(),
            "EffectId3".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: "some result 3".ToUtf8Bytes(),
            StoredException: null
        );
        
        await store.SetEffectResults(storedId, [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)]);
        await store.SetEffectResults(
            storedId,
            changes: [
                storedEffect3.ToStoredChange(storedId, Insert), 
                StoredEffectChange.CreateDelete(storedId, storedEffect1.StoredEffectId), 
                StoredEffectChange.CreateDelete(storedId, storedEffect2.StoredEffectId)
            ]
        );
        
        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(1);
        var effect3 = effects.Single(e => e.EffectId == storedEffect3.EffectId);
        effect3.Result.ShouldBe("some result 3".ToUtf8Bytes());
    }
    
    public abstract Task BulkDeleteTest();
    protected async Task BulkDeleteTest(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            "EffectId1".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            "EffectId2".ToStoredEffectId(EffectType.Effect),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );

        await store.SetEffectResults(
            storedId,
            changes: [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)]
        );
        await store.SetEffectResults(
            storedId,
            changes: [
                StoredEffectChange.CreateDelete(storedId, storedEffect1.StoredEffectId), 
                StoredEffectChange.CreateDelete(storedId, storedEffect2.StoredEffectId)
            ]
        );
        
        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(0);
    }
    
    public abstract Task UpsertEmptyCollectionOfEffectsDoesNotThrowException();
    protected async Task UpsertEmptyCollectionOfEffectsDoesNotThrowException(Task<IEffectsStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        await store.SetEffectResults(
            storedId,
            changes: []
        );
    }
}