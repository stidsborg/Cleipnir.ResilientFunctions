using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private record EffectKey(StoredEffectId EffectId, bool IsState);
    private readonly Dictionary<StoredId, Dictionary<EffectKey, StoredEffect>> _effects = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_sync)
            _effects.Clear();

        return Task.CompletedTask;
    }

    public Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        lock (_sync)
        {
            var key = new EffectKey(storedEffect.StoredEffectId, storedEffect.EffectId.IsState);
            if (!_effects.ContainsKey(storedId))
                _effects[storedId] = new Dictionary<EffectKey, StoredEffect>();
                
            _effects[storedId][key] = storedEffect;
        }
        
        return Task.CompletedTask;
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects)
    {
        foreach (var storedEffect in storedEffects)
            await SetEffectResult(storedId, storedEffect);
    }

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        lock (_sync)
            return !_effects.ContainsKey(storedId)
                ? ((IReadOnlyList<StoredEffect>) new List<StoredEffect>()).ToTask()
                : ((IReadOnlyList<StoredEffect>) _effects[storedId].Values.ToList()).ToTask();
    }

    public Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
    {
        var key = new EffectKey(effectId, isState);
        lock (_sync)
            if (_effects.ContainsKey(storedId))
                _effects[storedId].Remove(key);

        return Task.CompletedTask;
    }
    
    public Task Remove(StoredId storedId)
    {
        lock (_sync)
            _effects.Remove(storedId);

        return Task.CompletedTask;
    }
}