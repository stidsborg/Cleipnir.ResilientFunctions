using System;
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
        var sfAfterRestart = await functionStore.RestartExecution(functionId, existingEpoch, DateTime.UtcNow.Ticks);
        return sfAfterRestart != null;
    }
}