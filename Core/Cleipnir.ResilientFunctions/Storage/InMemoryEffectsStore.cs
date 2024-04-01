using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<FunctionId, Dictionary<EffectId, StoredEffect>> _effects = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task SetEffectResult(FunctionId functionId, StoredEffect storedEffect)
    {
        lock (_sync)
        {
            if (!_effects.ContainsKey(functionId))
                _effects[functionId] = new Dictionary<EffectId, StoredEffect>();
                
            _effects[functionId][storedEffect.EffectId] = storedEffect;
        }
        
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEffect>> GetEffectResults(FunctionId functionId)
    {
        lock (_sync)
            return !_effects.ContainsKey(functionId)
                ? Enumerable.Empty<StoredEffect>().ToTask()
                : _effects[functionId].Values.ToList().AsEnumerable().ToTask();
    }

    public Task DeleteEffectResult(FunctionId functionId, EffectId effectId)
    {
        lock (_sync)
            if (_effects.ContainsKey(functionId))
                _effects[functionId].Remove(effectId);

        return Task.CompletedTask;
    }

    public Task Remove(FunctionId functionId)
    {
        lock (_sync)
            _effects.Remove(functionId);

        return Task.CompletedTask;
    }
}