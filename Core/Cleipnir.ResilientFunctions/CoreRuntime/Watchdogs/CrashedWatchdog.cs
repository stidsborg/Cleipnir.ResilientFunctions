using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly Restarter _restarter;

    public CrashedWatchdog(RestarterFactory restarterFactory)
    {
        _restarter = restarterFactory.Create(
            (flowType, store, t) => store.GetCrashedFunctions(flowType, leaseExpiresBefore: t)
        );
    }

    public Task Start() => _restarter.Start(nameof(CrashedWatchdog));
}