﻿using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(FlowId flowId, StoredEffect storedEffect);
    Task<IReadOnlyList<StoredEffect>> GetEffectResults(FlowId flowId);
    Task DeleteEffectResult(FlowId flowId, EffectId effectId, bool isState);
    Task Remove(FlowId flowId);
}