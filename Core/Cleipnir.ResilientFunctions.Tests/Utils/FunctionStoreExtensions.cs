using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class FunctionStoreExtensions
{
    public static async Task<bool> IncrementEpoch(this IFunctionStore functionStore, FlowId flowId)
    {
        var sf = await functionStore.GetFunction(flowId);
        var existingEpoch = sf!.Epoch;
        var sfAfterRestart = await functionStore.RestartExecution(flowId, existingEpoch, DateTime.UtcNow.Ticks);
        return sfAfterRestart != null;
    }
}