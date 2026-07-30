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
    internal TimeSpan? WatchdogCheckFrequency { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? MessagesPullFrequency { get; }
    internal TimeSpan? MessagesDefaultMaxWaitForCompletion { get; }
    internal ISerializer? Serializer { get; }
    internal UtcNow? UtcNow { get; }
    internal TimeSpan? ReplicaHeartbeatFrequency { get; }
    internal TimeSpan? UnregisteredFlowTypesGracePeriod { get; }

    public Settings(
        Action<FrameworkException>? unhandledExceptionHandler = null,
        TimeSpan? retentionPeriod = null,
        TimeSpan? retentionCleanUpFrequency = null,
        TimeSpan? watchdogCheckFrequency = null,
        TimeSpan? messagesPullFrequency = null,
        TimeSpan? messagesDefaultMaxWaitForCompletion = null,
        int? maxParallelRetryInvocations = null,
        ISerializer? serializer = null,
        UtcNow? utcNow = null,
        TimeSpan? replicaHeartbeatFrequency = null,
        TimeSpan? unregisteredFlowTypesGracePeriod = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        RetentionPeriod = retentionPeriod;
        RetentionCleanUpFrequency = retentionCleanUpFrequency;
        WatchdogCheckFrequency = watchdogCheckFrequency;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        MessagesPullFrequency = messagesPullFrequency;
        MessagesDefaultMaxWaitForCompletion = messagesDefaultMaxWaitForCompletion;
        UtcNow = utcNow;
        if (replicaHeartbeatFrequency.HasValue && replicaHeartbeatFrequency.Value < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException("ReplicaHeartbeatFrequency must be greater than zero");

        ReplicaHeartbeatFrequency = replicaHeartbeatFrequency;
        UnregisteredFlowTypesGracePeriod = unregisteredFlowTypesGracePeriod;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan RetentionPeriod,
    TimeSpan RetentionCleanUpFrequency,
    TimeSpan WatchdogCheckFrequency,
    TimeSpan MessagesPullFrequency,
    TimeSpan MessagesDefaultMaxWaitForCompletion,
    int MaxParallelRetryInvocations,
    ISerializer Serializer,
    UtcNow UtcNow,
    TimeSpan ReplicaHeartbeatFrequency,
    TimeSpan UnregisteredFlowTypesGracePeriod)
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
            child.WatchdogCheckFrequency ?? WatchdogCheckFrequency,
            child.MessagesPullFrequency ?? MessagesPullFrequency,
            child.MessagesDefaultMaxWaitForCompletion ?? MessagesDefaultMaxWaitForCompletion,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer,
            child.UtcNow ?? (() => DateTime.UtcNow),
            child.ReplicaHeartbeatFrequency ?? ReplicaHeartbeatFrequency,
            child.UnregisteredFlowTypesGracePeriod ?? UnregisteredFlowTypesGracePeriod
        );
    }
    
    public SettingsWithDefaults Merge(LocalSettings? child)
    {
        if (child == null) return this;
        
        return this with
        {
            RetentionPeriod = child.RetentionPeriod ?? RetentionPeriod, 
            MessagesDefaultMaxWaitForCompletion = child.MessagesDefaultMaxWaitForCompletion ?? MessagesDefaultMaxWaitForCompletion, 
            MaxParallelRetryInvocations = child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations
        };
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            RetentionPeriod: TimeSpan.MaxValue,
            RetentionCleanUpFrequency: TimeSpan.FromHours(1),
            WatchdogCheckFrequency: TimeSpan.FromSeconds(1),
            MessagesPullFrequency: TimeSpan.FromMilliseconds(250),
            MessagesDefaultMaxWaitForCompletion: TimeSpan.Zero, 
            MaxParallelRetryInvocations: 1000,
            Serializer: DefaultSerializer.Instance,
            UtcNow: () => DateTime.UtcNow,
            ReplicaHeartbeatFrequency: TimeSpan.FromSeconds(1),
            UnregisteredFlowTypesGracePeriod: TimeSpan.FromMinutes(10)
        );
}