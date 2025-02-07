using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<StoredId, Dictionary<StoredEffectId, StoredEffect>> _effects = new();
    private readonly Lock _sync = new();

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
            if (!_effects.ContainsKey(storedId))
                _effects[storedId] = new Dictionary<StoredEffectId, StoredEffect>();
                
            _effects[storedId][storedEffect.StoredEffectId] = storedEffect;
        }
        
        return Task.CompletedTask;
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> upsertEffects, IReadOnlyList<StoredEffectId> removeEffects)
    {
        foreach (var storedEffect in upsertEffects)
            await SetEffectResult(storedId, storedEffect);

        foreach (var effectId in removeEffects)
            await DeleteEffectResult(storedId, effectId);
    }

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        lock (_sync)
            return !_effects.ContainsKey(storedId)
                ? ((IReadOnlyList<StoredEffect>) new List<StoredEffect>()).ToTask()
                : ((IReadOnlyList<StoredEffect>) _effects[storedId].Values.ToList()).ToTask();
    }

    public Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId)
    {
        lock (_sync)
            if (_effects.ContainsKey(storedId))
                _effects[storedId].Remove(effectId);

        return Task.CompletedTask;
    }
    
    public Task Remove(StoredId storedId)
    {
        lock (_sync)
            _effects.Remove(storedId);

        return Task.CompletedTask;
    }
}