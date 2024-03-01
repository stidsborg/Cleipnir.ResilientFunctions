using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IEffectsStore
{
    Task Initialize();
    Task SetEffectResult(FunctionId functionId, StoredEffect storedEffect);
    Task<IEnumerable<StoredEffect>> GetEffectResults(FunctionId functionId);
    Task DeleteEffectResult(FunctionId functionId, string effectId);
}