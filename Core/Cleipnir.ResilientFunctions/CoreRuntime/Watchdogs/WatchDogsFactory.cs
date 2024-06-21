using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore,
        ReInvoke reInvoke, 
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator)
    {
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            asyncSemaphore,
            settings.LeaseLength,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            functionStore,
            reInvoke,
            asyncSemaphore,
            settings.WatchdogCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var messagesWriters = new MessageWriters(
            functionTypeId,
            functionStore,
            settings.Serializer,
            scheduleReInvocation: (id, epoch) => reInvoke(id, epoch)
        );
        var timeoutWatchdog = new TimeoutWatchdog(
            functionTypeId,
            messagesWriters,
            functionStore.TimeoutStore,
            settings.WatchdogCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        Task.Run(crashedWatchdog.Start);
        Task.Run(postponedWatchdog.Start);
        Task.Run(timeoutWatchdog.Start);
    }
}