using System;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions;

public record Settings(
    Action<RFunctionException>? UnhandledExceptionHandler = null,
    TimeSpan? CrashedCheckFrequency = null,
    TimeSpan? PostponedCheckFrequency = null,
    TimeSpan? DelayStartup = null,
    int? MaxParallelRetryInvocations = null,
    ISerializer? Serializer = null
);

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan CrashedCheckFrequency,
    TimeSpan PostponedCheckFrequency,
    TimeSpan DelayStartup,
    int MaxParallelRetryInvocations,
    ISerializer Serializer
)
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
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 10,
            Serializer: DefaultSerializer.Instance
        );
}