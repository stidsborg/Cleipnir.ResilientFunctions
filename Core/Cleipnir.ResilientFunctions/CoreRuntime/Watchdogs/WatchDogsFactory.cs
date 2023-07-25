using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke, 
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator)
    {
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            asyncSemaphore,
            settings.SignOfLifeFrequency,
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
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var eventSourceWriters = new EventSourceWriters(
            functionTypeId,
            functionStore.EventStore,
            settings.Serializer,
            scheduleReInvocation: (id, epoch, status) => reInvoke(id, epoch, status)
        );
        var timeoutWatchdog = new TimeoutWatchdog(
            functionTypeId,
            eventSourceWriters,
            functionStore.TimeoutStore,
            settings.TimeoutCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        Task.Run(crashedWatchdog.Start);
        Task.Run(postponedWatchdog.Start);
        Task.Run(timeoutWatchdog.Start);
    }
}