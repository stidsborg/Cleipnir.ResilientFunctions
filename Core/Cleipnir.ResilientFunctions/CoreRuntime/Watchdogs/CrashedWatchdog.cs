using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly Restarter _restarter;

    public CrashedWatchdog(RestarterFactory restarterFactory)
    {
        _restarter = restarterFactory.Create(
            (functionTypeId, store, t) => store.GetCrashedFunctions(functionTypeId, leaseExpiresBefore: t)
        );
    }

    public Task Start() => _restarter.Start(nameof(CrashedWatchdog));
}