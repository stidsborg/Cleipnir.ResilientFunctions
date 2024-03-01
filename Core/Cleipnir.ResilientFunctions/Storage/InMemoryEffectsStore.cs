using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<FunctionId, Dictionary<string, StoredEffect>> _activities = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task SetEffectResult(FunctionId functionId, StoredEffect storedEffect)
    {
        lock (_sync)
        {
            if (!_activities.ContainsKey(functionId))
                _activities[functionId] = new Dictionary<string, StoredEffect>();
                
            _activities[functionId][storedEffect.EffectId] = storedEffect;
        }
        
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredEffect>> GetEffectResults(FunctionId functionId)
    {
        lock (_sync)
            return !_activities.ContainsKey(functionId)
                ? Enumerable.Empty<StoredEffect>().ToTask()
                : _activities[functionId].Values.ToList().AsEnumerable().ToTask();
    }

    public Task DeleteEffectResult(FunctionId functionId, string effectId)
    {
        lock (_sync)
            if (_activities.ContainsKey(functionId))
                _activities[functionId].Remove(effectId);

        return Task.CompletedTask;
    }
}