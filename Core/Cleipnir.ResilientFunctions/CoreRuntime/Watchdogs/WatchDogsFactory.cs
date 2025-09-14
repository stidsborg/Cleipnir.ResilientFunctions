using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FlowType flowType, 
        StoredType storedType,
        IFunctionStore functionStore,
        CrashedOrPostponedWatchdog crashedOrPostponedWatchdog,
        Restart restart, 
        RestartFunction restartFunction,
        ScheduleRestartFromWatchdog scheduleRestart,
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator, 
        ISerializer serializer,
        UtcNow utcNow)
    {
        if (!settings.EnableWatchdogs)
            return;

        if (settings.WatchdogCheckFrequency == TimeSpan.Zero || settings.WatchdogCheckFrequency == TimeSpan.MaxValue)
            throw new InvalidOperationException(nameof(Settings.WatchdogCheckFrequency) + " is invalid");
        
        var asyncSemaphore = new AsyncSemaphore(settings.MaxParallelRetryInvocations);

        crashedOrPostponedWatchdog.Register(
            storedType,
            restartFunction,
            scheduleRestart,
            asyncSemaphore
        );

        var retentionWatchdog = new RetentionWatchdog(
            flowType,
            storedType,
            functionStore,
            settings.RetentionCleanUpFrequency,
            settings.DelayStartup,
            settings.RetentionPeriod,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator,
            utcNow
        );
        
        Task.Run(retentionWatchdog.Start);
    }
}