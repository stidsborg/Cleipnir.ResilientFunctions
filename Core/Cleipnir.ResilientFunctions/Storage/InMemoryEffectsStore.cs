using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<FlowId, Dictionary<EffectId, StoredEffect>> _effects = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_sync)
            _effects.Clear();

        return Task.CompletedTask;
    }

    public Task SetEffectResult(FlowId flowId, StoredEffect storedEffect)
    {
        lock (_sync)
        {
            if (!_effects.ContainsKey(flowId))
                _effects[flowId] = new Dictionary<EffectId, StoredEffect>();
                
            _effects[flowId][storedEffect.EffectId] = storedEffect;
        }
        
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEffect>> GetEffectResults(FlowId flowId)
    {
        lock (_sync)
            return !_effects.ContainsKey(flowId)
                ? Enumerable.Empty<StoredEffect>().ToTask()
                : _effects[flowId].Values.ToList().AsEnumerable().ToTask();
    }

    public Task DeleteEffectResult(FlowId flowId, EffectId effectId)
    {
        lock (_sync)
            if (_effects.ContainsKey(flowId))
                _effects[flowId].Remove(effectId);

        return Task.CompletedTask;
    }

    public Task Remove(FlowId flowId)
    {
        lock (_sync)
            _effects.Remove(flowId);

        return Task.CompletedTask;
    }
}