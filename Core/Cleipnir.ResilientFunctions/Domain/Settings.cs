using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? SignOfLifeFrequency { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? TimeoutCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal TimeSpan? EventSourcePullFrequency { get; }
    internal ISerializer? Serializer { get; }

    public Settings(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? signOfLifeFrequency = null, 
        TimeSpan? postponedCheckFrequency = null,
        TimeSpan? timeoutCheckFrequency = null,
        TimeSpan? eventSourcePullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        SignOfLifeFrequency = signOfLifeFrequency;
        PostponedCheckFrequency = postponedCheckFrequency;
        TimeoutCheckFrequency = timeoutCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        EventSourcePullFrequency = eventSourcePullFrequency;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan SignOfLifeFrequency,
    TimeSpan PostponedCheckFrequency,
    TimeSpan TimeoutCheckFrequency,
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
            child.SignOfLifeFrequency ?? SignOfLifeFrequency,
            child.PostponedCheckFrequency ?? PostponedCheckFrequency,
            child.TimeoutCheckFrequency ?? TimeoutCheckFrequency,
            child.EventSourcePullFrequency ?? EventSourcePullFrequency,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer
        );
    }

    public static SettingsWithDefaults Default { get; }
        = new(
            UnhandledExceptionHandler: new UnhandledExceptionHandler(_ => {}),
            SignOfLifeFrequency: TimeSpan.FromSeconds(10),
            PostponedCheckFrequency: TimeSpan.FromSeconds(10),
            TimeoutCheckFrequency: TimeSpan.FromSeconds(10),
            EventSourcePullFrequency: TimeSpan.FromMilliseconds(250),
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 10,
            Serializer: DefaultSerializer.Instance
        );
}