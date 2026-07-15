using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

/// <summary>
/// Test-only convenience. Production code reads a flow's effects via <see cref="IFunctionStore.GetFunction"/>
/// (<c>StoredFlow.Effects</c>); there is deliberately no standalone effect-read on the store. These extensions
/// mirror the former <c>GetEffectResults</c> so the effect-persistence tests can assert on stored effects directly.
/// </summary>
public static class FunctionStoreEffectTestExtensions
{
    public static async Task<IReadOnlyList<StoredEffect>> GetEffectResults(this IFunctionStore store, StoredId storedId)
        => (await store.GetFunction(storedId))?.Effects ?? [];

    public static async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(this IFunctionStore store, IEnumerable<StoredId> storedIds)
    {
        var result = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            result[storedId] = ((await store.GetFunction(storedId))?.Effects ?? []).ToList();

        return result;
    }
}
