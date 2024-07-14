using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly Restarter _restarter;

    public PostponedWatchdog(RestarterFactory restarterFactory)
        => _restarter = restarterFactory.Create(
            getEligibleFunctions: (flowType, store, t) => store.GetPostponedFunctions(flowType, isEligibleBefore: t) 
        );


    public Task Start() => _restarter.Start(nameof(PostponedWatchdog));
}