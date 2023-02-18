using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class FunctionStoreExtensions
{
    public static async Task<bool> IncrementEpoch(this IFunctionStore functionStore, FunctionId functionId)
    {
        var sf = await functionStore.GetFunction(functionId);
        var existingEpoch = sf!.Epoch;
        return await functionStore.IncrementPostponedFunctionEpoch(functionId, existingEpoch);
    }
}