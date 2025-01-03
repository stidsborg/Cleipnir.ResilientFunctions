﻿using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(StoredId storedId, StoredEffect storedEffect);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId);
    Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId);
    Task Remove(StoredId storedId);
}