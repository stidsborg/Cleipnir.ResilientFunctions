using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly Restarter _restarter;

    public PostponedWatchdog(RestarterFactory restarterFactory)
        => _restarter = restarterFactory.Create(
            getEligibleFunctions: (functionTypeId, store, t) => store.GetPostponedFunctions(functionTypeId, isEligibleBefore: t) 
        );


    public Task Start() => _restarter.Start(nameof(PostponedWatchdog));
}