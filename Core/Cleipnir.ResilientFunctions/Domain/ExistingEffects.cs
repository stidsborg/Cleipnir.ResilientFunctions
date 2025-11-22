using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEffects(StoredId storedId, FlowId flowId, IEffectsStore effectsStore, ISerializer serializer)
{
    private Dictionary<EffectId, StoredEffect>? _storedEffects;

    private async Task<Dictionary<EffectId, StoredEffect>> GetStoredEffects()
    {
        if (_storedEffects is not null)
            return _storedEffects;

        var storedEffects = await effectsStore.GetEffectResults(storedId);
        return _storedEffects = storedEffects.ToDictionary(e => e.EffectId, e => e);
    }
    
    public Task<IEnumerable<EffectId>> AllIds 
        => GetStoredEffects().ContinueWith(t => (IEnumerable<EffectId>) t.Result.Keys);

    public async Task<bool> HasValue(int effectId) => (await GetStoredEffects()).ContainsKey(effectId.ToEffectId());

    public async Task<TResult?> GetValue<TResult>(int effectId) => await GetValue<TResult>(effectId.ToEffectId());
    public async Task<TResult?> GetValue<TResult>(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        var success = storedEffects.TryGetValue(effectId, out var storedEffect);
        if (!success)
            throw new KeyNotFoundException($"Effect '{effectId}' was not found");
        if (storedEffect!.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Effect '{effectId}' has not completed (but has status '{storedEffect.WorkStatus}')");

        return storedEffect.Result == null 
            ? default 
            : serializer.Deserialize<TResult>(storedEffects[effectId].Result!);
    }

    public async Task<byte[]?> GetResultBytes(int effectId) => await GetResultBytes(effectId.ToEffectId());
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
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: serializer.Serialize(result), StoredException: null, Alias: null));

    public Task SetFailed(int effectId, Exception exception) => SetFailed(effectId.ToEffectId(), exception);
    public Task SetFailed(EffectId effectId, Exception exception)
        => Set(new StoredEffect(effectId, WorkStatus.Failed, Result: null, StoredException: serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, exception)), Alias: null));
}