using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? LeaseLength { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? TimeoutEventsCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? EventSourcePullFrequency { get; }
    internal ISerializer? Serializer { get; }

    public Settings(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? leaseLength = null, 
        TimeSpan? postponedCheckFrequency = null,
        TimeSpan? timeoutEventsCheckFrequency = null,
        TimeSpan? eventSourcePullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        LeaseLength = leaseLength;
        PostponedCheckFrequency = postponedCheckFrequency;
        TimeoutEventsCheckFrequency = timeoutEventsCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        EventSourcePullFrequency = eventSourcePullFrequency;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan LeaseLength,
    TimeSpan PostponedCheckFrequency,
    TimeSpan TimeoutEventsCheckFrequency,
    TimeSpan EventSourcePullFrequency,
    TimeSpan DelayStartup,
    int MaxParallelRetryInvocations,
    ISerializer Serializer)
{
    public SettingsWithDefaults Merge(Settings? child)
    {
        if (child == null) return this;
        
        return new SettingsWithDefaults(
            child.UnhandledExceptionHandler == null
                ? UnhandledExceptionHandler
                : new UnhandledExceptionHandler(child.UnhandledExceptionHandler),
            child.LeaseLength ?? LeaseLength,
            child.PostponedCheckFrequency ?? PostponedCheckFrequency,
            child.TimeoutEventsCheckFrequency ?? TimeoutEventsCheckFrequency,
            child.EventSourcePullFrequency ?? EventSourcePullFrequency,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer
        );
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            LeaseLength: TimeSpan.FromSeconds(10),
            PostponedCheckFrequency: TimeSpan.FromSeconds(1),
            TimeoutEventsCheckFrequency: TimeSpan.FromSeconds(1),
            EventSourcePullFrequency: TimeSpan.FromMilliseconds(250),
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 100,
            Serializer: DefaultSerializer.Instance
        );
}