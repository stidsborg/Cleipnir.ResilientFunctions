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
        int version,
        ShutdownCoordinator shutdownCoordinator)
    {
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            asyncSemaphore,
            settings.CrashedCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            asyncSemaphore,
            settings.PostponedCheckFrequency,
            settings.CrashedCheckFrequency,
            settings.DelayStartup,
            version,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
    }
}