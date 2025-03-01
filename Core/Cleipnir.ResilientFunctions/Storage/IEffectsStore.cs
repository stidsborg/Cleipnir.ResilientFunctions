using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffect storedEffect);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId);
    Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId);
    Task DeleteEffectResults(StoredId storedId, IReadOnlyList<StoredEffectId> effectIds);
    Task Remove(StoredId storedId);
}