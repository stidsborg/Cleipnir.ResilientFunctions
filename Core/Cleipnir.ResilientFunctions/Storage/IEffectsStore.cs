using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task Truncate();
    Task SetEffectResult(FlowId flowId, StoredEffect storedEffect);
    Task<IEnumerable<StoredEffect>> GetEffectResults(FlowId flowId);
    Task DeleteEffectResult(FlowId flowId, EffectId effectId);
    Task Remove(FlowId flowId);
}