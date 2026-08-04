using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal static class WatchDogsFactory
{
    public static void CreateAndStart(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        PostponedWatchdog postponedWatchdog,
        SettingsWithDefaults settings,
        ShutdownCoordinator shutdownCoordinator,
        UtcNow utcNow)
    {
        if (settings.WatchdogCheckFrequency == TimeSpan.Zero || settings.WatchdogCheckFrequency == TimeSpan.MaxValue)
            throw new InvalidOperationException(nameof(Settings.WatchdogCheckFrequency) + " is invalid");

        postponedWatchdog.Register(storedType);

        var retentionWatchdog = new RetentionWatchdog(
            flowType,
            storedType,
            functionStore,
            settings.RetentionCleanUpFrequency,
            settings.RetentionPeriod,
            settings.UnhandledExceptionHandler,
            shutdownCoordinator,
            utcNow
        );
        
        Task.Run(retentionWatchdog.Start);
    }
}