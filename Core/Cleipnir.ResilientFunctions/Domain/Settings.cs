using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? RetentionPeriod { get; }
    internal TimeSpan? LeaseLength { get; }
    internal bool? EnableWatchdogs { get; }
    internal TimeSpan? WatchdogCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? MessagesPullFrequency { get; }
    internal ISerializer? Serializer { get; }
    internal IEnumerable<RoutingInformation>? Routes { get; }

    public Settings(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? retentionPeriod = null,
        TimeSpan? leaseLength = null, 
        bool? enableWatchdogs = null,
        TimeSpan? watchdogCheckFrequency = null,
        TimeSpan? messagesPullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null,
        IEnumerable<RoutingInformation>? routes = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        RetentionPeriod = retentionPeriod;
        LeaseLength = leaseLength;
        EnableWatchdogs = enableWatchdogs;
        WatchdogCheckFrequency = watchdogCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        MessagesPullFrequency = messagesPullFrequency;
        Routes = routes;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan RetentionPeriod,
    TimeSpan LeaseLength,
    bool EnableWatchdogs,
    TimeSpan WatchdogCheckFrequency,
    TimeSpan MessagesPullFrequency,
    TimeSpan DelayStartup,
    int MaxParallelRetryInvocations,
    ISerializer Serializer,
    IEnumerable<RoutingInformation> Routes)
{
    public SettingsWithDefaults Merge(Settings? child)
    {
        if (child == null) return this;
        
        return new SettingsWithDefaults(
            child.UnhandledExceptionHandler == null
                ? UnhandledExceptionHandler
                : new UnhandledExceptionHandler(child.UnhandledExceptionHandler),
            child.RetentionPeriod ?? RetentionPeriod,
            child.LeaseLength ?? LeaseLength,
            child.EnableWatchdogs ?? EnableWatchdogs,
            child.WatchdogCheckFrequency ?? WatchdogCheckFrequency,
            child.MessagesPullFrequency ?? MessagesPullFrequency,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer,
            child.Routes ?? Routes
        );
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            RetentionPeriod: TimeSpan.MaxValue,
            LeaseLength: TimeSpan.FromSeconds(10),
            EnableWatchdogs: true,
            WatchdogCheckFrequency: TimeSpan.FromSeconds(1),
            MessagesPullFrequency: TimeSpan.FromMilliseconds(250),
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 100,
            Serializer: DefaultSerializer.Instance,
            Routes: []
        );
}