using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly ReInvoker _reInvoker;

    public CrashedWatchdog(ReInvokerFactory reInvokerFactory)
    {
        _reInvoker = reInvokerFactory.Create(
            (functionTypeId, store, t) => store.GetCrashedFunctions(functionTypeId, leaseExpiresBefore: t)
        );
    }

    public Task Start() => _reInvoker.Start(nameof(CrashedWatchdog));
}