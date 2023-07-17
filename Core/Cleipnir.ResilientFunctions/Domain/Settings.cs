using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? CrashedCheckFrequency { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? TimeoutCheckFrequency { get; }
    internal TimeSpan? SuspensionCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? EventSourcePullFrequency { get; }
    internal ISerializer? Serializer { get; }

    public Settings(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? crashedCheckFrequency = null, 
        TimeSpan? postponedCheckFrequency = null,
        TimeSpan? timeoutCheckFrequency = null,
        TimeSpan? suspensionCheckFrequency = null,
        TimeSpan? eventSourcePullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        CrashedCheckFrequency = crashedCheckFrequency;
        PostponedCheckFrequency = postponedCheckFrequency;
        TimeoutCheckFrequency = timeoutCheckFrequency;
        SuspensionCheckFrequency = suspensionCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        EventSourcePullFrequency = eventSourcePullFrequency;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan CrashedCheckFrequency,
    TimeSpan PostponedCheckFrequency,
    TimeSpan TimeoutCheckFrequency,
    TimeSpan SuspensionCheckFrequency,
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
            child.CrashedCheckFrequency ?? CrashedCheckFrequency,
            child.PostponedCheckFrequency ?? PostponedCheckFrequency,
            child.TimeoutCheckFrequency ?? TimeoutCheckFrequency,
            child.SuspensionCheckFrequency ?? SuspensionCheckFrequency,
            child.EventSourcePullFrequency ?? EventSourcePullFrequency,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer
        );
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            CrashedCheckFrequency: TimeSpan.FromSeconds(10),
            PostponedCheckFrequency: TimeSpan.FromSeconds(10),
            TimeoutCheckFrequency: TimeSpan.FromSeconds(10),
            SuspensionCheckFrequency: TimeSpan.FromSeconds(10), 
            EventSourcePullFrequency: TimeSpan.FromMilliseconds(250),
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 10,
            Serializer: DefaultSerializer.Instance
        );
}