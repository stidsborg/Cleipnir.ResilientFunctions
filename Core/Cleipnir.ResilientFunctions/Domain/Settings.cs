using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class Settings
{
    internal Action<FrameworkException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? RetentionPeriod { get; }
    internal TimeSpan? RetentionCleanUpFrequency { get; }
    internal TimeSpan? LeaseLength { get; }
    internal bool? EnableWatchdogs { get; }
    internal TimeSpan? WatchdogCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? MessagesPullFrequency { get; }
    internal TimeSpan? MessagesDefaultMaxWaitForCompletion { get; }
    internal ISerializer? Serializer { get; }
    internal UtcNow? UtcNow { get; }

    public Settings(
        Action<FrameworkException>? unhandledExceptionHandler = null, 
        TimeSpan? retentionPeriod = null,
        TimeSpan? retentionCleanUpFrequency = null,
        TimeSpan? leaseLength = null,
        bool? enableWatchdogs = null,
        TimeSpan? watchdogCheckFrequency = null,
        TimeSpan? messagesPullFrequency = null,
        TimeSpan? messagesDefaultMaxWaitForCompletion = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null,
        UtcNow? utcNow = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        RetentionPeriod = retentionPeriod;
        RetentionCleanUpFrequency = retentionCleanUpFrequency;
        LeaseLength = leaseLength;
        EnableWatchdogs = enableWatchdogs;
        WatchdogCheckFrequency = watchdogCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        MessagesPullFrequency = messagesPullFrequency;
        MessagesDefaultMaxWaitForCompletion = messagesDefaultMaxWaitForCompletion;
        UtcNow = utcNow;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan RetentionPeriod,
    TimeSpan RetentionCleanUpFrequency,
    TimeSpan LeaseLength,
    bool EnableWatchdogs,
    TimeSpan WatchdogCheckFrequency,
    TimeSpan MessagesPullFrequency,
    TimeSpan MessagesDefaultMaxWaitForCompletion,
    TimeSpan DelayStartup,
    int MaxParallelRetryInvocations,
    ISerializer Serializer,
    UtcNow UtcNow)
{
    public SettingsWithDefaults Merge(Settings? child)
    {
        if (child == null) return this;
        
        return new SettingsWithDefaults(
            child.UnhandledExceptionHandler == null
                ? UnhandledExceptionHandler
                : new UnhandledExceptionHandler(child.UnhandledExceptionHandler),
            child.RetentionPeriod ?? RetentionPeriod,
            child.RetentionCleanUpFrequency ?? RetentionCleanUpFrequency,
            child.LeaseLength ?? LeaseLength,
            child.EnableWatchdogs ?? EnableWatchdogs,
            child.WatchdogCheckFrequency ?? WatchdogCheckFrequency,
            child.MessagesPullFrequency ?? MessagesPullFrequency,
            child.MessagesDefaultMaxWaitForCompletion ?? MessagesDefaultMaxWaitForCompletion,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer,
            child.UtcNow ?? (() => DateTime.UtcNow)
        );
    }
    
    public SettingsWithDefaults Merge(LocalSettings? child)
    {
        if (child == null) return this;
        
        return this with
        {
            RetentionPeriod = child.RetentionPeriod ?? RetentionPeriod, 
            EnableWatchdogs = child.EnableWatchdogs ?? EnableWatchdogs, 
            MessagesDefaultMaxWaitForCompletion = child.MessagesDefaultMaxWaitForCompletion ?? MessagesDefaultMaxWaitForCompletion, 
            MaxParallelRetryInvocations = child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations
        };
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            RetentionPeriod: TimeSpan.MaxValue,
            RetentionCleanUpFrequency: TimeSpan.FromHours(1), 
            LeaseLength: TimeSpan.FromSeconds(60),
            EnableWatchdogs: true,
            WatchdogCheckFrequency: TimeSpan.FromSeconds(1),
            MessagesPullFrequency: TimeSpan.FromMilliseconds(250),
            MessagesDefaultMaxWaitForCompletion: TimeSpan.Zero, 
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 100,
            Serializer: DefaultSerializer.Instance,
            UtcNow: () => DateTime.UtcNow
        );
}