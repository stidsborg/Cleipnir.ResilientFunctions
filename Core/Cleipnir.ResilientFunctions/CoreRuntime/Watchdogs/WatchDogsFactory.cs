using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FlowType flowType, 
        IFunctionStore functionStore,
        ReInvoke reInvoke, 
        RestartFunction restartFunction,
        ScheduleReInvokeFromWatchdog scheduleReInvoke,
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator)
    {
        if (!settings.EnableWatchdogs)
            return;

        if (settings.WatchdogCheckFrequency == TimeSpan.Zero || settings.WatchdogCheckFrequency == TimeSpan.MaxValue)
            throw new InvalidOperationException(nameof(Settings.WatchdogCheckFrequency) + " is invalid");
        
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);
        var reInvokerFactory = new ReInvokerFactory(
            flowType,
            functionStore,
            shutdownCoordinator,
            settings.UnhandledExceptionHandler,
            settings.WatchdogCheckFrequency,
            settings.DelayStartup,
            asyncSemaphore,
            restartFunction,
            scheduleReInvoke
        );
        
        var crashedWatchdog = new CrashedWatchdog(reInvokerFactory);
        var postponedWatchdog = new PostponedWatchdog(reInvokerFactory);

        var messagesWriters = new MessageWriters(
            flowType,
            functionStore,
            settings.Serializer,
            scheduleReInvocation: (id, epoch) => reInvoke(id, epoch)
        );
        
        var timeoutWatchdog = new TimeoutWatchdog(
            flowType,
            messagesWriters,
            functionStore.TimeoutStore,
            settings.WatchdogCheckFrequency,
            settings.DelayStartup,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        var retentionWatchdog = new RetentionWatchdog(
            flowType,
            functionStore,
            settings.RetentionCleanUpFrequency,
            settings.DelayStartup,
            settings.RetentionPeriod,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );

        Task.Run(crashedWatchdog.Start);
        Task.Run(postponedWatchdog.Start);
        Task.Run(timeoutWatchdog.Start);
        Task.Run(retentionWatchdog.Start);
    }
}