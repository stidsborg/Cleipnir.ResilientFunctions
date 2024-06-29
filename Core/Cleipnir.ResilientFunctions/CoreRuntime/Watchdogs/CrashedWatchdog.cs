using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly TimeSpan _leaseLength;
    private readonly Restarter _restarter;

    public CrashedWatchdog(TimeSpan leaseLength, RestarterFactory restarterFactory)
    {
        _leaseLength = leaseLength;
        _restarter = restarterFactory.Create(
            (functionTypeId, store, t) => store.GetCrashedAndEligiblePostponedFunctions(functionTypeId, nowTimestamp: t)
        );
    }

    public async Task Start()
    {
        await _restarter.Start(nameof(CrashedWatchdog));
    }
}