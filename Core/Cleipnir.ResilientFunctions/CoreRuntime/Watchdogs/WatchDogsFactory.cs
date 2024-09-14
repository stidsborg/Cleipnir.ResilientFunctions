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
        TimeoutWatchdog timeoutWatchdog,
        CrashedOrPostponedWatchdog crashedOrPostponedWatchdog,
        Restart restart, 
        RestartFunction restartFunction,
        ScheduleRestartFromWatchdog scheduleRestart,
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator)
    {
        if (!settings.EnableWatchdogs)
            return;

        if (settings.WatchdogCheckFrequency == TimeSpan.Zero || settings.WatchdogCheckFrequency == TimeSpan.MaxValue)
            throw new InvalidOperationException(nameof(Settings.WatchdogCheckFrequency) + " is invalid");
        
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);

        crashedOrPostponedWatchdog.Register(
            flowType,
            restartFunction,
            scheduleRestart,
            asyncSemaphore
        );

        var messagesWriters = new MessageWriters(
            flowType,
            functionStore,
            settings.Serializer,
            scheduleReInvocation: (id, epoch) => restart(id, epoch)
        );
        timeoutWatchdog.Register(flowType, messagesWriters);

        var retentionWatchdog = new RetentionWatchdog(
            flowType,
            functionStore,
            settings.RetentionCleanUpFrequency,
            settings.DelayStartup,
            settings.RetentionPeriod,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator
        );
        
        Task.Run(retentionWatchdog.Start);
    }
}