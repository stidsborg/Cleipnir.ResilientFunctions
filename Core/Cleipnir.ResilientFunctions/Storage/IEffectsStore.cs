using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffect storedEffect);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects) => SetEffectResults(storedId, storedEffects, removeEffects: Array.Empty<StoredEffectId>());
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> upsertEffects, IReadOnlyList<StoredEffectId> removeEffects);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId);
    Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId);
    Task DeleteEffectResults(StoredId storedId, IReadOnlyList<StoredEffectId> effectIds);
    Task Remove(StoredId storedId);
}