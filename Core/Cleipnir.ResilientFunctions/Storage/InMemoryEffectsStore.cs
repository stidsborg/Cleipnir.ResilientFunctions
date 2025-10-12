using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Session;

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

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        lock (_sync)
        {
            foreach (var storedEffect in changes.Where(c => c.Operation != CrudOperation.Delete).Select(c => c.StoredEffect!))
            {
                if (!_effects.ContainsKey(storedId))
                    _effects[storedId] = new Dictionary<StoredEffectId, StoredEffect>();

                _effects[storedId][storedEffect.StoredEffectId] = storedEffect;
            }

            foreach (var effectId in changes.Where(c => c.Operation == CrudOperation.Delete).Select(c => c.EffectId))
                if (_effects.ContainsKey(storedId))
                    _effects[storedId].Remove(effectId.ToStoredEffectId());
        }

        return Task.CompletedTask;
    }

    public Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        var dict = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            lock (_sync)
                dict[storedId] = _effects.ContainsKey(storedId)
                    ? _effects[storedId].Values.ToList()
                    : new List<StoredEffect>();    
        
        return dict.ToTask();
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