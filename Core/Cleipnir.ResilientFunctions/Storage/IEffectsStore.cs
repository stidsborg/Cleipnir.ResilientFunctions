using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffect storedEffect);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId);
    Task DeleteEffectResult(StoredId storedId, EffectId effectId, bool isState);
    Task Remove(StoredId storedId);
}