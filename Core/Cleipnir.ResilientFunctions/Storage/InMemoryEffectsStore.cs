using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryEffectsStore : IEffectsStore
{
    private readonly Dictionary<StoredId, Dictionary<EffectId, StoredEffect>> _effects = new();
    private readonly Dictionary<StoredId, int> _versions = new();
    private readonly Lock _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_sync)
        {
            _effects.Clear();
            _versions.Clear();
        }

        return Task.CompletedTask;
    }

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return Task.CompletedTask;

        lock (_sync)
        {
            var storageSession = session as SnapshotStorageSession;

            if (storageSession is { RowExists: true })
                if (_versions.ContainsKey(storedId) && storageSession.Version != _versions[storedId])
                    return Task.CompletedTask;

            if (storageSession is { RowExists: false } && _effects.ContainsKey(storedId))
                return Task.CompletedTask;
            
            if (!_effects.ContainsKey(storedId))
            {
                _effects[storedId] = new Dictionary<EffectId, StoredEffect>();
                _versions[storedId] = 0;
            }
            
            foreach (var change in changes.Where(c => c.Operation != CrudOperation.Delete))
                _effects[storedId][change.EffectId] = change.StoredEffect!;
            foreach (var change in changes.Where(c => c.Operation == CrudOperation.Delete))
                _effects[storedId].Remove(change.EffectId);
            
            _versions[storedId]++;
            if (storageSession != null)
            {
                storageSession.Version++;
                storageSession.RowExists = true;
                foreach (var change in changes)
                    if (change.Operation == CrudOperation.Delete)
                        storageSession.Effects.Remove(change.EffectId);
                    else
                        storageSession.Effects[change.EffectId] = change.StoredEffect!;
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
        {
            _effects.Remove(storedId);
            _versions.Remove(storedId);
        }

        return Task.CompletedTask;
    }

    public int GetVersion(StoredId storedId)
    {
        lock (_sync)
            return _versions.GetValueOrDefault(storedId, 0);
    }
}