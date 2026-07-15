using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

internal static class StoreTestExtensions
{
    /// <summary>
    /// Test convenience over <see cref="IFunctionStore.RestartExecutions"/> that restarts a single flow and
    /// returns its restarted state (or null when the flow was not claimable). Like the batch restart, this only
    /// claims parked flows (postponed/suspended).
    /// </summary>
    public static async Task<StoredFlowWithEffects?> RestartExecution(this IFunctionStore store, StoredId storedId, ReplicaId owner)
        => (await store.RestartExecutions([storedId], owner)).GetValueOrDefault(storedId);
}
