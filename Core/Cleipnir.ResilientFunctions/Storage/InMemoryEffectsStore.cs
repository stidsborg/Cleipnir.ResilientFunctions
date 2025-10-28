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
            var rowExistsInStorage = _effects.ContainsKey(storedId);

            if (storageSession != null)
            {
                // Optimistic concurrency control
                if (rowExistsInStorage && !storageSession.RowExists)
                    return Task.CompletedTask; // Conflict: row exists but session doesn't know

                if (!rowExistsInStorage && storageSession.RowExists)
                    return Task.CompletedTask; // Conflict: session thinks row exists but it doesn't

                if (rowExistsInStorage && storageSession.RowExists)
                {
                    // Both agree row exists - check version
                    var currentVersion = _versions[storedId];
                    if (currentVersion != storageSession.Version)
                        return Task.CompletedTask; // Version mismatch

                    // Version matches - increment before applying changes
                    _versions[storedId]++;
                    storageSession.Version++;
                }
            }

            // Create row if needed
            if (!_effects.ContainsKey(storedId))
            {
                _effects[storedId] = new Dictionary<EffectId, StoredEffect>();
                _versions[storedId] = 0;
            }

            // Apply changes
            foreach (var change in changes)
            {
                if (change.Operation == CrudOperation.Delete)
                {
                    _effects[storedId].Remove(change.EffectId);
                    if (storageSession != null)
                        storageSession.Effects.Remove(change.EffectId);
                }
                else
                {
                    _effects[storedId][change.EffectId] = change.StoredEffect!;
                    if (storageSession != null)
                        storageSession.Effects[change.EffectId] = change.StoredEffect!;
                }
            }

            // Update session state if this was first insert
            if (storageSession != null && !storageSession.RowExists)
            {
                storageSession.RowExists = true;
                _versions[storedId] = 1;
                storageSession.Version = 1;
            }
            else if (storageSession == null && rowExistsInStorage)
            {
                // No session provided but row exists - increment version
                _versions[storedId]++;
            }
            else if (storageSession == null && !rowExistsInStorage)
            {
                // No session provided and new row - set version to 1
                _versions[storedId] = 1;
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