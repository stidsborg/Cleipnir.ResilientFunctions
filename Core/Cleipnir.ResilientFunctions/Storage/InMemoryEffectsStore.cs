using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<StoredId, Dictionary<long, StoredEffect>> _effects = new();
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
        var positionsSession = session as PositionsStorageSession ?? CreateStorageSession(storedId);
        
        lock (_sync)
        {
            if (!_effects.ContainsKey(storedId))
                _effects[storedId] = new Dictionary<long, StoredEffect>();
            
            foreach (var change in changes)
            {
                var position = positionsSession.Get(change.EffectId.Serialize());
                if (position != null)
                {
                    positionsSession.Remove(change.EffectId.Serialize());
                    _effects[storedId].Remove(position.Value);
                }
            }

            foreach (var change in changes.Where(c => c.Operation != CrudOperation.Delete))
            {
                var position = positionsSession.Add(change.EffectId.Serialize());
                _effects[storedId].Add(position, change.StoredEffect!);
            }
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

    public Task Remove(StoredId storedId)
    {
        lock (_sync)
            _effects.Remove(storedId);

        return Task.CompletedTask;
    }

    internal PositionsStorageSession CreateStorageSession(StoredId storedId)
    {
        var session = new PositionsStorageSession();
        lock (_sync)
        {
            if (!_effects.ContainsKey(storedId))
                return session;
            
            foreach(var (position, effect) in _effects[storedId])
                session.Set(effect.EffectId.Serialize(), position);
        }

        return session;
    }
}