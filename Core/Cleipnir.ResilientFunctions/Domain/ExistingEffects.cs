using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEffects(StoredId storedId, FlowId flowId, IEffectsStore effectsStore, ISerializer serializer, IReadOnlyList<StoredEffect> initialStoredEffects)
{
    private readonly Dictionary<EffectId, StoredEffect> _storedEffectsDict = 
        initialStoredEffects.ToDictionary(s => s.EffectId, s => s);

    private async Task<Dictionary<EffectId, StoredEffect>> GetStoredEffects()
    {
        await Task.CompletedTask;
        return _storedEffectsDict;
    }
    
    public Task<IEnumerable<EffectId>> AllIds => GetAllIds();
    private async Task<IEnumerable<EffectId>> GetAllIds()
    {
        var storedEffects = await GetStoredEffects();
        return storedEffects.Keys;
    }

    public async Task<bool> HasValue(int effectId)
    {
        var storedEffects = await GetStoredEffects();
        return storedEffects.ContainsKey(effectId.ToEffectId());
    }

    public Task<TResult?> GetValue<TResult>(int effectId) => GetValue<TResult>(effectId.ToEffectId());
    public async Task<TResult?> GetValue<TResult>(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        var success = storedEffects.TryGetValue(effectId, out var storedEffect);
        if (!success)
        {
            storedEffects = await GetStoredEffects();
            success = storedEffects.TryGetValue(effectId, out storedEffect);
            if (!success)
                throw new KeyNotFoundException($"Effect '{effectId}' was not found");
        }
        if (storedEffect!.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Effect '{effectId}' has not completed (but has status '{storedEffect.WorkStatus}')");

        return storedEffect.Result == null
            ? default
            : (TResult)serializer.Deserialize(storedEffects[effectId].Result!, typeof(TResult));
    }

    public Task<byte[]?> GetResultBytes(int effectId) => GetResultBytes(effectId.ToEffectId());
    public async Task<byte[]?> GetResultBytes(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        return storedEffects[effectId].Result;
    }

    public async Task<WorkStatus> GetStatus(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        return storedEffects[effectId].WorkStatus;
    }

    public async Task RemoveFailed()
    {
        var allIds = (await AllIds).ToList();
        var failedEffectIds = new HashSet<SerializedEffectId>();

        // First pass: identify failed effects and their serialized IDs
        foreach (var effectId in allIds)
            if (await GetStatus(effectId) == WorkStatus.Failed)
                failedEffectIds.Add(effectId.Serialize());

        // Second pass: remove failed effects and their children
        foreach (var effectId in allIds)
        {
            var isFailedEffect = failedEffectIds.Contains(effectId.Serialize());
            var parentSerializedId = effectId.Context.Length > 0
                ? new EffectId(effectId.Context).Serialize()
                : null;
            var isChildOfFailedEffect = parentSerializedId != null && failedEffectIds.Contains(parentSerializedId);

            if (isFailedEffect || isChildOfFailedEffect)
                await Remove(effectId);
        }
    }

    public Task Remove(int effectId) => Remove(effectId.ToEffectId());
    public async Task Remove(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        await effectsStore.DeleteEffectResult(storedId, effectId, storageSession: null);
        storedEffects.Remove(effectId);
    }

    private async Task Set(StoredEffect storedEffect)
    {
        var storedEffects = await GetStoredEffects();
        var crudOperation = storedEffects.ContainsKey(storedEffect.EffectId)
            ? CrudOperation.Update
            : CrudOperation.Insert;
        var change = new StoredEffectChange(storedId, storedEffect.EffectId, crudOperation, storedEffect);
        await effectsStore.SetEffectResult(storedId, change, session: null);
        storedEffects[storedEffect.EffectId] = storedEffect;
    }

    public Task SetValue<TValue>(int effectId, TValue value) => SetValue(effectId.ToEffectId(), value);
    public Task SetValue<TValue>(EffectId effectId, TValue value) => SetSucceeded(effectId, value);

    public Task SetStarted(int effectId) => SetStarted(effectId.ToEffectId());
    public Task SetStarted(EffectId effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Started, Result: null, StoredException: null, Alias: null));

    public Task SetSucceeded(int effectId) => SetSucceeded(effectId.ToEffectId());
    public Task SetSucceeded(EffectId effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null, Alias: null));

    public Task SetSucceeded<TResult>(int effectId, TResult result) => SetSucceeded(effectId.ToEffectId(), result);
    public Task SetSucceeded<TResult>(EffectId effectId, TResult result)
    {
        var serializedResult = serializer.Serialize(result!, typeof(TResult));
        return Set(new StoredEffect(effectId, WorkStatus.Completed, Result: serializedResult, StoredException: null, Alias: null));
    }

    public Task SetFailed(int effectId, Exception exception) => SetFailed(effectId.ToEffectId(), exception);
    public Task SetFailed(EffectId effectId, Exception exception)
        => Set(new StoredEffect(effectId, WorkStatus.Failed, Result: null, StoredException: FatalWorkflowException.CreateNonGeneric(flowId, exception).ToStoredException(), Alias: null));

    public string EffectTree()
    {
        var pendingChanges = _storedEffectsDict.ToDictionary(
            kvp => kvp.Key,
            kvp => new PendingEffectChange(kvp.Key, kvp.Value, Operation: null, Existing: true, kvp.Value.Alias)
        );
        return EffectPrinter.Print(pendingChanges);
    }
}