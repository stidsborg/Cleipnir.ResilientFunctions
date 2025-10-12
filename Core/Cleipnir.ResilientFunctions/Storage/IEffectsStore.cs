using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffect storedEffect, IStorageSession? session);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId);
    Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds);
    Task DeleteEffectResult(StoredId storedId, EffectId effectId, IStorageSession? storageSession)
        => SetEffectResults(
            storedId,
            changes: [new StoredEffectChange(storedId, effectId, CrudOperation.Delete, StoredEffect: null)],
            storageSession
        );
    Task Remove(StoredId storedId);
}