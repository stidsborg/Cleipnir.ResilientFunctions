using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEffects(FlowId flowId, IEffectsStore effectsStore, ISerializer serializer)
{
    private Dictionary<EffectId, StoredEffect>? _storedEffects;

    private async Task<Dictionary<EffectId, StoredEffect>> GetStoredEffects()
    {
        if (_storedEffects is not null)
            return _storedEffects;

        var storedEffects = await effectsStore.GetEffectResults(flowId);
        return _storedEffects = storedEffects.ToDictionary(e => e.EffectId, e => e);
    }
    
    public Task<IEnumerable<EffectId>> AllIds 
        => GetStoredEffects().ContinueWith(t => (IEnumerable<EffectId>) t.Result.Keys);

    public async Task<bool> HasValue(string effectId) => (await GetStoredEffects()).ContainsKey(effectId);
    public async Task<TResult?> GetValue<TResult>(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        var success = storedEffects.TryGetValue(effectId, out var storedEffect);
        if (!success)
            throw new KeyNotFoundException($"Effect '{effectId}' was not found");
        if (storedEffect!.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Effect '{effectId}' has not completed (but has status '{storedEffect.WorkStatus}')");

        return storedEffect.Result == null 
            ? default 
            : serializer.DeserializeEffectResult<TResult>(storedEffects[effectId].Result!);
    } 
    
    public async Task Remove(string effectId)
    {
        var storedEffects = await GetStoredEffects();
        await effectsStore.DeleteEffectResult(flowId, effectId);
        storedEffects.Remove(effectId);
    }

    private async Task Set(StoredEffect storedEffect)
    {
        var storedEffects = await GetStoredEffects();
        await effectsStore.SetEffectResult(flowId, storedEffect);
        storedEffects[storedEffect.EffectId] = storedEffect;
    }

    public Task SetValue<TValue>(string effectId, TValue value) => SetSucceeded(effectId, value);

    public Task SetStarted(string effectId) 
        => Set(new StoredEffect(effectId, WorkStatus.Started, Result: null, StoredException: null));
    
    public Task SetSucceeded(string effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null));
    
    public Task SetSucceeded<TResult>(string effectId, TResult result)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: serializer.SerializeEffectResult(result), StoredException: null));

    public Task SetFailed(string effectId, Exception exception)
        => Set(new StoredEffect(effectId, WorkStatus.Failed, Result: null, StoredException: serializer.SerializeException(exception)));
}