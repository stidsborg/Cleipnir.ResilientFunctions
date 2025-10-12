using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffectChange storedEffectChange, IStorageSession? session)
        => SetEffectResults(storedId, changes: [storedEffectChange], session);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session);
    async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId) 
        => (await GetEffectResults([storedId]))[storedId];
    Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds);
    Task DeleteEffectResult(StoredId storedId, EffectId effectId, IStorageSession? storageSession)
        => SetEffectResults(
            storedId,
            changes: [new StoredEffectChange(storedId, effectId, CrudOperation.Delete, StoredEffect: null)],
            storageSession
        );
    Task Remove(StoredId storedId);
}