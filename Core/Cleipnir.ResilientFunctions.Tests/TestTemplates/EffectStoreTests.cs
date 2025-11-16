using System;
using System.Linq;
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
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store
            .GetEffectResults(functionId)
            .SelectAsync(l => l.Any())
            .ShouldBeFalseAsync();
        
        await store.SetEffectResult(functionId, storedEffect1.ToStoredChange(functionId, Insert), session: null);

        var storedEffects = await store
            .GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        var se = storedEffects[0];
        se.ShouldBe(storedEffect1);

        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Insert), session: null);
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Any(s => s == storedEffect1).ShouldBeTrue();
        storedEffects.Any(s => s == storedEffect2).ShouldBeTrue();

        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Update), session: null);
        await store.GetEffectResults(functionId);

        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Update), session: null);
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Any(s => s == storedEffect1).ShouldBeTrue();
        storedEffects.Any(s => s == storedEffect2).ShouldBeTrue();
    }
    
    public abstract Task SingleEffectWithResultLifeCycle();
    protected async Task SingleEffectWithResultLifeCycle(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var effect = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetEffectResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();

        await store.SetEffectResult(functionId, effect.ToStoredChange(functionId, Insert), session: null);
        var storedEffect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        storedEffect.ShouldBe(effect);

        effect = effect with { WorkStatus = WorkStatus.Completed, Result = "Hello World".ToUtf8Bytes() };
        await store.SetEffectResult(functionId, effect.ToStoredChange(functionId, Update), session: null);
        storedEffect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());

        storedEffect.EffectId.ShouldBe(effect.EffectId);
        storedEffect.StoredException.ShouldBe(effect.StoredException);
        storedEffect.Result!.ToStringFromUtf8Bytes().ShouldBe(effect.Result.ToStringFromUtf8Bytes());
        storedEffect.WorkStatus.ShouldBe(effect.WorkStatus);

    }
    
    public abstract Task SingleFailingEffectLifeCycle();
    protected async Task SingleFailingEffectLifeCycle(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedException = new StoredException(
            "Some Exception Message",
            "SomeStackTrace",
            "Some Exception Type"
        );
        var storedEffect = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetEffectResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();

        await store.SetEffectResult(functionId, storedEffect.ToStoredChange(functionId, Insert), session: null);
        var effect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        effect.ShouldBe(storedEffect);

        storedEffect = storedEffect with { WorkStatus = WorkStatus.Completed, StoredException = storedException };
        await store.SetEffectResult(functionId, storedEffect.ToStoredChange(functionId, Update), session: null);
        effect = await store.GetEffectResults(functionId).SelectAsync(r => r.Single());
        effect.ShouldBe(storedEffect);
    }
    
    public abstract Task EffectCanBeDeleted();
    protected async Task EffectCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store.SetEffectResult(functionId, storedEffect1.ToStoredChange(functionId, Insert), session: null);
        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Insert), session: null);

        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Count() == 2)
            .ShouldBeTrueAsync();

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId, storageSession: null);
        var storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);

        await store.DeleteEffectResult(functionId, storedEffect2.EffectId, storageSession: null);
        storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects[0].EffectId.ShouldBe(storedEffect1.EffectId);

        await store.DeleteEffectResult(functionId, storedEffect1.EffectId, storageSession: null);
        await store
            .GetEffectResults(functionId)
            .SelectAsync(sas => sas.Any())
            .ShouldBeFalseAsync();
    }
    
    public abstract Task DeleteFunctionIdDeletesAllRelatedEffects();
    protected async Task DeleteFunctionIdDeletesAllRelatedEffects(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var otherFunctionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        await functionStore.CreateFunction(
            otherFunctionId,
            "OtherInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;

        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );

        await store.SetEffectResult(functionId, storedEffect1.ToStoredChange(functionId, Insert), session: null);
        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Insert), session: null);
        await store.SetEffectResult(otherFunctionId, storedEffect1.ToStoredChange(otherFunctionId, Insert), session: null);

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
    protected async Task TruncateDeletesAllEffects(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var otherFunctionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        await functionStore.CreateFunction(
            otherFunctionId,
            "OtherInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;

        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );

        await store.SetEffectResult(functionId, storedEffect1.ToStoredChange(functionId, Insert), session: null);
        await store.SetEffectResult(functionId, storedEffect2.ToStoredChange(functionId, Insert), session: null);
        await store.SetEffectResult(otherFunctionId, storedEffect1.ToStoredChange(otherFunctionId, Insert), session: null);

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
    protected async Task BulkInsertTest(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            storedId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );

        await store.SetEffectResults(
            storedId,
            [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)],
            session: null
        );

        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(2);
        var effect1 = effects.Single(e => e.EffectId == storedEffect1.EffectId);
        effect1.Result.ShouldBe("some result 1".ToUtf8Bytes());
        var effect2 = effects.Single(e => e.EffectId == storedEffect2.EffectId);
        effect2.Result.ShouldBe("some result 2".ToUtf8Bytes());
    }
    
    public abstract Task BulkInsertAndDeleteTest();
    protected async Task BulkInsertAndDeleteTest(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            storedId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect3 = new StoredEffect(
            "EffectId3".ToEffectId(),
            WorkStatus.Completed,
            Result: "some result 3".ToUtf8Bytes(),
            StoredException: null
        );

        await store.SetEffectResults(storedId, [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)], session: null);
        await store.SetEffectResults(
            storedId,
            changes: [
                storedEffect3.ToStoredChange(storedId, Insert),
                StoredEffectChange.CreateDelete(storedId, storedEffect1.EffectId),
                StoredEffectChange.CreateDelete(storedId, storedEffect2.EffectId)
            ],
            session: null
        );

        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(1);
        var effect3 = effects.Single(e => e.EffectId == storedEffect3.EffectId);
        effect3.Result.ShouldBe("some result 3".ToUtf8Bytes());
    }
    
    public abstract Task BulkDeleteTest();
    protected async Task BulkDeleteTest(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            storedId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: "some result 1".ToUtf8Bytes(),
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: "some result 2".ToUtf8Bytes(),
            StoredException: null
        );

        await store.SetEffectResults(
            storedId,
            changes: [storedEffect1.ToStoredChange(storedId, Insert), storedEffect2.ToStoredChange(storedId, Insert)],
            session: null
        );
        await store.SetEffectResults(
            storedId,
            changes: [
                StoredEffectChange.CreateDelete(storedId, storedEffect1.EffectId),
                StoredEffectChange.CreateDelete(storedId, storedEffect2.EffectId)
            ],
            session: null
        );

        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(0);
    }
    
    public abstract Task UpsertEmptyCollectionOfEffectsDoesNotThrowException();
    protected async Task UpsertEmptyCollectionOfEffectsDoesNotThrowException(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            storedId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        await store.SetEffectResults(
            storedId,
            changes: [],
            session: null
        );
    }
    
    public abstract Task EffectsForDifferentIdsCanBeFetched();
    protected async Task EffectsForDifferentIdsCanBeFetched(Task<IFunctionStore> storeTask)
    {
        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            id1,
            "HumanInstanceId1",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        await functionStore.CreateFunction(
            id2,
            "HumanInstanceId2",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId2".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );

        await store.SetEffectResult(id1, storedEffect1.ToStoredChange(id1, Insert), session: null);
        await store.SetEffectResult(id1, storedEffect2.ToStoredChange(id1, Insert), session: null);
        await store.SetEffectResult(id2, storedEffect1.ToStoredChange(id2, Insert), session: null);
        await store.SetEffectResult(id2, storedEffect2.ToStoredChange(id2, Insert), session: null);

        var results = await store.GetEffectResults([id1, id2]);
        results.Count.ShouldBe(2);
        var resultsId1 = results[id1];
        resultsId1.Count.ShouldBe(2);
        resultsId1.Any(r => r.EffectId == storedEffect1.EffectId).ShouldBeTrue();
        resultsId1.Any(r => r.EffectId == storedEffect2.EffectId).ShouldBeTrue();

        var resultsId2 = results[id2];
        resultsId2.Count.ShouldBe(2);
        resultsId2.Any(r => r.EffectId == storedEffect1.EffectId).ShouldBeTrue();
        resultsId2.Any(r => r.EffectId == storedEffect2.EffectId).ShouldBeTrue();
    }
    
    public abstract Task OverwriteExistingEffectWorks();
    protected async Task OverwriteExistingEffectWorks(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var effectStore = store.EffectsStore;
        var storedId = TestStoredId.Create();
        var storageSession = await store.CreateFunction(
            storedId,
            "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );

        await effectStore.SetEffectResult(storedId, storedEffect1.ToStoredChange(storedId, Insert), storageSession);
        await effectStore.SetEffectResult(storedId, storedEffect2.ToStoredChange(storedId, Update), storageSession);

        var storedEffects = await effectStore.GetEffectResults(storedId);
        var storedEffect = storedEffects.Single();
        storedEffect.EffectId.ShouldBe("EffectId1".ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
    }
    
    public abstract Task StoreCanHandleMultipleEffectsWithSameIdOnDifferentSessions();
    protected async Task StoreCanHandleMultipleEffectsWithSameIdOnDifferentSessions(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var effectStore = store.EffectsStore;
        var storedId = TestStoredId.Create();
        var crashingReplicaId = ReplicaId.NewId();
        var storageSession1 = await store.CreateFunction(
            storedId,
            "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: crashingReplicaId
        );

        await store.RescheduleCrashedFunctions(crashingReplicaId);
        var storageSession2 = await store.RestartExecution(
            storedId,
            owner: ReplicaId.NewId()
        ).SelectAsync(s => s!.StorageSession);
        
        var storedEffect1 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var storedEffect2 = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        
        try
        {
            await effectStore.SetEffectResult(storedId, storedEffect1.ToStoredChange(storedId, Insert), storageSession1);
        }
        catch (Exception)
        {
            // ignored
        }

        try
        {
            await effectStore.SetEffectResult(storedId, storedEffect2.ToStoredChange(storedId, Insert),
                storageSession2);
        }
        catch (Exception)
        {
            //ignored
        }
        
        
        var storedEffects = await effectStore.GetEffectResults(storedId);
        var storedEffect = storedEffects.Single();
        storedEffect.EffectId.ShouldBe("EffectId1".ToEffectId());
        (storedEffect.WorkStatus is WorkStatus.Completed or WorkStatus.Started).ShouldBeTrue();
    }

    public abstract Task MultipleSequentialUpdatesWithoutRefresh();
    protected async Task MultipleSequentialUpdatesWithoutRefresh(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var effectStore = store.EffectsStore;
        var storedId = TestStoredId.Create();

        // Create initial session
        var session = await store.CreateFunction(storedId, "instance", null, 0, null, 0, null, ReplicaId.NewId());

        // Sequential updates - each should increment version properly
        var effect1 = new StoredEffect("Effect1".ToEffectId(), WorkStatus.Completed, "result1".ToUtf8Bytes(), null);
        await effectStore.SetEffectResult(storedId, effect1.ToStoredChange(storedId, Insert), session);

        var effect2 = new StoredEffect("Effect2".ToEffectId(), WorkStatus.Completed, "result2".ToUtf8Bytes(), null);
        await effectStore.SetEffectResult(storedId, effect2.ToStoredChange(storedId, Insert), session);

        var effect3 = new StoredEffect("Effect3".ToEffectId(), WorkStatus.Completed, "result3".ToUtf8Bytes(), null);
        await effectStore.SetEffectResult(storedId, effect3.ToStoredChange(storedId, Insert), session);

        // Verify all three effects were persisted
        var effects = await effectStore.GetEffectResults(storedId);
        effects.Count.ShouldBe(3);
        effects.Any(e => e.EffectId == effect1.EffectId).ShouldBeTrue();
        effects.Any(e => e.EffectId == effect2.EffectId).ShouldBeTrue();
        effects.Any(e => e.EffectId == effect3.EffectId).ShouldBeTrue();
    }

    public abstract Task StoreHandlesLargeNumberOfEffects();
    protected async Task StoreHandlesLargeNumberOfEffects(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var effectStore = store.EffectsStore;
        var storedId = TestStoredId.Create();

        // Create initial session
        var session = await store.CreateFunction(storedId, "instance", null, 0, null, 0, null, ReplicaId.NewId());

        // Insert 100 effects
        const int effectCount = 100;
        for (int i = 0; i < effectCount; i++)
        {
            var effect = new StoredEffect(
                $"Effect{i}".ToEffectId(),
                WorkStatus.Completed,
                $"result{i}".ToUtf8Bytes(),
                null
            );
            await effectStore.SetEffectResult(storedId, effect.ToStoredChange(storedId, Insert), session);
        }

        // Verify all effects were persisted
        var effects = await effectStore.GetEffectResults(storedId);
        effects.Count.ShouldBe(effectCount);

        // Verify a few specific effects
        effects.Any(e => e.EffectId == "Effect0".ToEffectId()).ShouldBeTrue();
        effects.Any(e => e.EffectId == "Effect50".ToEffectId()).ShouldBeTrue();
        effects.Any(e => e.EffectId == "Effect99".ToEffectId()).ShouldBeTrue();
    }

    public abstract Task EffectsSerializeAndDeserializeCorrectly();
    protected async Task EffectsSerializeAndDeserializeCorrectly(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            storedId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;

        // Create effects with various edge cases
        var effectWithNullResult = new StoredEffect(
            "NullResult".ToEffectId(),
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        var effectWithLargeResult = new StoredEffect(
            "LargeResult".ToEffectId(),
            WorkStatus.Completed,
            Result: new byte[10000], // 10KB
            StoredException: null
        );

        var effectWithException = new StoredEffect(
            "WithException".ToEffectId(),
            WorkStatus.Failed,
            Result: null,
            StoredException: new StoredException(
                "Message with special chars: \n\t\r\"'\\",
                "Stack trace with\nmultiple\nlines",
                "System.InvalidOperationException"
            )
        );

        var effectWithSpecialChars = new StoredEffect(
            "SpecialCharsInResult".ToEffectId(),
            WorkStatus.Completed,
            Result: "Special chars: 🚀 \n\t\r\"'\\".ToUtf8Bytes(),
            StoredException: null
        );

        // Insert all effects
        await store.SetEffectResult(storedId, effectWithNullResult.ToStoredChange(storedId, Insert), null);
        await store.SetEffectResult(storedId, effectWithLargeResult.ToStoredChange(storedId, Insert), null);
        await store.SetEffectResult(storedId, effectWithException.ToStoredChange(storedId, Insert), null);
        await store.SetEffectResult(storedId, effectWithSpecialChars.ToStoredChange(storedId, Insert), null);

        // Retrieve and verify
        var effects = await store.GetEffectResults(storedId);
        effects.Count.ShouldBe(4);

        var retrievedNull = effects.Single(e => e.EffectId == "NullResult".ToEffectId());
        retrievedNull.Result.ShouldBeNull();
        retrievedNull.StoredException.ShouldBeNull();

        var retrievedLarge = effects.Single(e => e.EffectId == "LargeResult".ToEffectId());
        retrievedLarge.Result.ShouldNotBeNull();
        retrievedLarge.Result!.Length.ShouldBe(10000);

        var retrievedException = effects.Single(e => e.EffectId == "WithException".ToEffectId());
        retrievedException.StoredException.ShouldNotBeNull();
        retrievedException.StoredException!.ExceptionMessage.ShouldBe("Message with special chars: \n\t\r\"'\\");

        var retrievedSpecial = effects.Single(e => e.EffectId == "SpecialCharsInResult".ToEffectId());
        retrievedSpecial.Result!.ToStringFromUtf8Bytes().ShouldBe("Special chars: 🚀 \n\t\r\"'\\");
    }

    public abstract Task MixedInsertUpdateDeleteInSequence();
    protected async Task MixedInsertUpdateDeleteInSequence(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var effectStore = store.EffectsStore;
        var storedId = TestStoredId.Create();

        // Create initial session
        var session = await store.CreateFunction(storedId, "instance", null, 0, null, 0, null, ReplicaId.NewId());

        // INSERT effect1
        var effect1 = new StoredEffect("Effect1".ToEffectId(), WorkStatus.Started, null, null);
        await effectStore.SetEffectResult(storedId, effect1.ToStoredChange(storedId, Insert), session);

        // UPDATE effect1
        effect1 = effect1 with { WorkStatus = WorkStatus.Completed, Result = "done".ToUtf8Bytes() };
        await effectStore.SetEffectResult(storedId, effect1.ToStoredChange(storedId, Update), session);

        // DELETE effect2 (doesn't exist - should be no-op)
        await effectStore.DeleteEffectResult(storedId, "Effect2".ToEffectId(), session);

        // INSERT effect3
        var effect3 = new StoredEffect("Effect3".ToEffectId(), WorkStatus.Completed, null, null);
        await effectStore.SetEffectResult(storedId, effect3.ToStoredChange(storedId, Insert), session);

        // UPDATE effect1 again
        effect1 = effect1 with { Result = "done2".ToUtf8Bytes() };
        await effectStore.SetEffectResult(storedId, effect1.ToStoredChange(storedId, Update), session);

        // INSERT effect2
        var effect2 = new StoredEffect("Effect2".ToEffectId(), WorkStatus.Started, null, null);
        await effectStore.SetEffectResult(storedId, effect2.ToStoredChange(storedId, Insert), session);

        // DELETE effect3
        await effectStore.DeleteEffectResult(storedId, effect3.EffectId, session);

        // Verify final state: effect1 (updated) and effect2 should exist, effect3 should be deleted
        var effects = await effectStore.GetEffectResults(storedId);
        effects.Count.ShouldBe(2);

        var finalEffect1 = effects.Single(e => e.EffectId == "Effect1".ToEffectId());
        finalEffect1.WorkStatus.ShouldBe(WorkStatus.Completed);
        finalEffect1.Result!.ToStringFromUtf8Bytes().ShouldBe("done2");

        effects.Any(e => e.EffectId == "Effect2".ToEffectId()).ShouldBeTrue();
        effects.Any(e => e.EffectId == "Effect3".ToEffectId()).ShouldBeFalse();
    }

    public abstract Task EffectWithAliasCanBePersistedAndFetched();
    protected async Task EffectWithAliasCanBePersistedAndFetched(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await storeTask;
        await functionStore.CreateFunction(
            functionId,
            "HumanInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var store = functionStore.EffectsStore;

        // Create effect with alias
        var effectWithAlias = new StoredEffect(
            "EffectId1".ToEffectId(),
            WorkStatus.Completed,
            Result: "Hello World".ToUtf8Bytes(),
            StoredException: null,
            Alias: "MyAlias"
        );

        // Persist effect
        await store.SetEffectResult(functionId, effectWithAlias.ToStoredChange(functionId, Insert), session: null);

        // Fetch effect
        var storedEffects = await store.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);

        var retrievedEffect = storedEffects[0];
        retrievedEffect.EffectId.ShouldBe(effectWithAlias.EffectId);
        retrievedEffect.WorkStatus.ShouldBe(effectWithAlias.WorkStatus);
        retrievedEffect.Result!.ToStringFromUtf8Bytes().ShouldBe("Hello World");
        retrievedEffect.Alias.ShouldBe("MyAlias");
    }
}