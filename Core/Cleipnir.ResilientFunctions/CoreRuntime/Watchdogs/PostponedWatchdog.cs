using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly Restarter _restarter;

    public PostponedWatchdog(RestarterFactory restarterFactory)
        => _restarter = restarterFactory.Create(
            getEligibleFunctions: (flowType, store) => store.GetPostponedFunctions(flowType, isEligibleBefore: DateTime.UtcNow.Ticks) 
        );


    public Task Start() => _restarter.Start(nameof(PostponedWatchdog));
}