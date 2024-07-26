using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly Restarter _restarter;

    public CrashedWatchdog(RestarterFactory restarterFactory)
    {
        _restarter = restarterFactory.Create(
            (flowType, store) => store.GetCrashedFunctions(flowType, leaseExpiresBefore: DateTime.UtcNow.Ticks)
        );
    }

    public Task Start() => _restarter.Start(nameof(CrashedWatchdog));
}