using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingEffects
{
    private readonly FunctionId _functionId;
    private readonly Dictionary<EffectId, StoredEffect> _storedEffects;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;

    public ExistingEffects(FunctionId functionId, Dictionary<EffectId, StoredEffect> storedEffects, IEffectsStore effectsStore, ISerializer serializer)
    {
        _functionId = functionId;
        _storedEffects = storedEffects;
        _effectsStore = effectsStore;
        _serializer = serializer;
    }

    public IReadOnlyDictionary<EffectId, StoredEffect> All => _storedEffects;

    public bool HasValue(string effectId) => _storedEffects.ContainsKey(effectId);
    public TResult? GetValue<TResult>(string effectId)
    {
        var success = _storedEffects.TryGetValue(effectId, out var storedEffect);
        if (!success)
            throw new KeyNotFoundException($"Effect '{effectId}' was not found");
        if (storedEffect!.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Effect '{effectId}' has not completed (but has status '{storedEffect.WorkStatus}')");

        return storedEffect.Result == null 
            ? default 
            : _serializer.DeserializeEffectResult<TResult>(_storedEffects[effectId].Result!);
    } 
    
    public async Task Remove(string effectId)
    {
        await _effectsStore.DeleteEffectResult(_functionId, effectId);
        _storedEffects.Remove(effectId);
    }

    private async Task Set(StoredEffect storedEffect) 
    {
        await _effectsStore.SetEffectResult(_functionId, storedEffect);
        _storedEffects[storedEffect.EffectId] = storedEffect;
    }

    public Task SetValue<TValue>(string effectId, TValue value) => SetSucceeded(effectId, value);

    public Task SetStarted(string effectId) 
        => Set(new StoredEffect(effectId, WorkStatus.Started, Result: null, StoredException: null));
    
    public Task SetSucceeded(string effectId)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null));
    
    public Task SetSucceeded<TResult>(string effectId, TResult result)
        => Set(new StoredEffect(effectId, WorkStatus.Completed, Result: _serializer.SerializeEffectResult(result), StoredException: null));

    public Task SetFailed(string effectId, Exception exception)
        => Set(new StoredEffect(effectId, WorkStatus.Failed, Result: null, StoredException: _serializer.SerializeException(exception)));
}