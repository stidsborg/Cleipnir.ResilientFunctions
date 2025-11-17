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

    public async Task<bool> HasValue(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        // First try to find by ID
        if (storedEffects.ContainsKey(effectId.ToEffectId()))
            return true;
        // Then try to find by alias
        return storedEffects.Values.Any(e => e.Alias == effectId);
    }

    public async Task<TResult?> GetValue<TResult>(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        // First try to find by ID
        var effectIdObj = effectId.ToEffectId();
        if (storedEffects.TryGetValue(effectIdObj, out var storedEffect))
            return await GetValueFromStoredEffect<TResult>(storedEffect, effectId);

        // Then try to find by alias
        storedEffect = storedEffects.Values.FirstOrDefault(e => e.Alias == effectId);
        if (storedEffect == null)
            throw new KeyNotFoundException($"Effect with ID or alias '{effectId}' was not found");

        return await GetValueFromStoredEffect<TResult>(storedEffect, effectId);
    }

    public async Task<TResult?> GetValue<TResult>(EffectId effectId)
    {
        var storedEffects = await GetStoredEffects();
        var success = storedEffects.TryGetValue(effectId, out var storedEffect);
        if (!success)
            throw new KeyNotFoundException($"Effect '{effectId}' was not found");

        return await GetValueFromStoredEffect<TResult>(storedEffect!, effectId.ToString());
    }

    private Task<TResult?> GetValueFromStoredEffect<TResult>(StoredEffect storedEffect, string identifier)
    {
        if (storedEffect.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Effect '{identifier}' has not completed (but has status '{storedEffect.WorkStatus}')");

        return Task.FromResult(storedEffect.Result == null
            ? default
            : serializer.Deserialize<TResult>(storedEffect.Result));
    }

    public async Task<byte[]?> GetResultBytes(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        // First try to find by ID
        var effectIdObj = effectId.ToEffectId();
        if (storedEffects.TryGetValue(effectIdObj, out var storedEffect))
            return storedEffect.Result;

        // Then try to find by alias
        storedEffect = storedEffects.Values.FirstOrDefault(e => e.Alias == effectId);
        if (storedEffect == null)
            throw new KeyNotFoundException($"Effect with ID or alias '{effectId}' was not found");

        return storedEffect.Result;
    }

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
        foreach (var effectId in await AllIds)
            if (await GetStatus(effectId) == WorkStatus.Failed || effectId.Type == EffectType.Retry)
                await Remove(effectId);
    }

    public async Task Remove(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        // First try to find by ID
        var effectIdObj = effectId.ToEffectId();
        if (storedEffects.ContainsKey(effectIdObj))
        {
            await Remove(effectIdObj);
            return;
        }

        // Then try to find by alias
        var storedEffect = storedEffects.FirstOrDefault(e => e.Value.Alias == effectId);
        if (storedEffect.Value == null)
            throw new KeyNotFoundException($"Effect with ID or alias '{effectId}' was not found");

        await Remove(storedEffect.Key);
    }

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

    public async Task SetValue<TValue>(string effectId, TValue value)
    {
        var effectIdObj = await GetEffectIdByIdOrAlias(effectId);
        await SetValue(effectIdObj, value);
    }

    public Task SetValue<TValue>(EffectId effectId, TValue value) => SetSucceeded(effectId, value);

    public async Task SetStarted(string effectId)
    {
        var effectIdObj = await GetEffectIdByIdOrAlias(effectId);
        await SetStarted(effectIdObj);
    }

    public Task SetStarted(EffectId effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Started, Result: null, StoredException: null, Alias: null));

    public async Task SetSucceeded(string effectId)
    {
        var effectIdObj = await GetEffectIdByIdOrAlias(effectId);
        await SetSucceeded(effectIdObj);
    }

    public Task SetSucceeded(EffectId effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null, Alias: null));

    public async Task SetSucceeded<TResult>(string effectId, TResult result)
    {
        var effectIdObj = await GetEffectIdByIdOrAlias(effectId);
        await SetSucceeded(effectIdObj, result);
    }

    public Task SetSucceeded<TResult>(EffectId effectId, TResult result)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: serializer.Serialize(result), StoredException: null, Alias: null));

    public async Task SetFailed(string effectId, Exception exception)
    {
        var effectIdObj = await GetEffectIdByIdOrAlias(effectId);
        await SetFailed(effectIdObj, exception);
    }

    public Task SetFailed(EffectId effectId, Exception exception)
        => Set(new StoredEffect(effectId, WorkStatus.Failed, Result: null, StoredException: serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, exception)), Alias: null));

    private async Task<EffectId> GetEffectIdByIdOrAlias(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        // First try to find by ID
        var effectIdObj = effectId.ToEffectId();
        if (storedEffects.ContainsKey(effectIdObj))
            return effectIdObj;

        // Then try to find by alias
        var storedEffect = storedEffects.FirstOrDefault(e => e.Value.Alias == effectId);
        if (storedEffect.Value == null)
            throw new KeyNotFoundException($"Effect with ID or alias '{effectId}' was not found");

        return storedEffect.Key;
    }
}