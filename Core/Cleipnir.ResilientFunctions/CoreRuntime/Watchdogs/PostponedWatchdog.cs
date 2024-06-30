using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly ReInvoker _reInvoker;

    public PostponedWatchdog(ReInvokerFactory reInvokerFactory)
        => _reInvoker = reInvokerFactory.Create(
            getEligibleFunctions: (functionTypeId, store, t) => store.GetPostponedFunctions(functionTypeId, isEligibleBefore: t) 
        );


    public Task Start() => _reInvoker.Start(nameof(PostponedWatchdog));
}