using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke, 
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator)
    {
        var workQueue = new WorkQueue(settings.MaxParallelRetryInvocations);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            workQueue,
            settings.CrashedCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            workQueue,
            settings.PostponedCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
    }
}