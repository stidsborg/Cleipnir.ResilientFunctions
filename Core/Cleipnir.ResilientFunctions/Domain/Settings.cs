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
    internal IDependencyResolver? DependencyResolver { get; }
    
    private readonly List<MiddlewareInstanceOrResolverFunc> _middlewares;
    internal IReadOnlyList<MiddlewareInstanceOrResolverFunc> Middlewares => _middlewares;

    public Settings(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? crashedCheckFrequency = null, 
        TimeSpan? postponedCheckFrequency = null,
        TimeSpan? timeoutCheckFrequency = null,
        TimeSpan? suspensionCheckFrequency = null,
        TimeSpan? eventSourcePullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null, 
        IDependencyResolver? dependencyResolver = null
    ) :this(
        unhandledExceptionHandler, crashedCheckFrequency, postponedCheckFrequency, timeoutCheckFrequency, suspensionCheckFrequency, eventSourcePullFrequency, 
        delayStartup, maxParallelRetryInvocations, serializer, dependencyResolver, 
        middlewares: new List<MiddlewareInstanceOrResolverFunc>()
    ) { }

    internal Settings(
        Action<RFunctionException>? unhandledExceptionHandler, 
        TimeSpan? crashedCheckFrequency, 
        TimeSpan? postponedCheckFrequency, 
        TimeSpan? timeoutCheckFrequency,
        TimeSpan? suspensionCheckFrequency,
        TimeSpan? eventSourcePullFrequency,
        TimeSpan? delayStartup, 
        int? maxParallelRetryInvocations, 
        ISerializer? serializer, 
        IDependencyResolver? dependencyResolver,
        List<MiddlewareInstanceOrResolverFunc> middlewares)
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        CrashedCheckFrequency = crashedCheckFrequency;
        PostponedCheckFrequency = postponedCheckFrequency;
        TimeoutCheckFrequency = timeoutCheckFrequency;
        SuspensionCheckFrequency = suspensionCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        DependencyResolver = dependencyResolver;
        _middlewares = middlewares;
        EventSourcePullFrequency = eventSourcePullFrequency;
    }

    public Settings UseMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
        if (DependencyResolver == null)
            throw new InvalidOperationException(
                $"{DependencyResolver} must be non-null when registering middleware using generic argument"
            );

        _middlewares.Add(
            new MiddlewareInstanceOrResolverFunc(
                Instance: null,
                Resolver: resolver => resolver.Resolve<TMiddleware>()
            )
        );

        return this;
    }

    public Settings UseMiddleware(IMiddleware middleware) 
    {
        _middlewares.Add(new MiddlewareInstanceOrResolverFunc(middleware, Resolver: null));
        return this;
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
    ISerializer Serializer,
    IDependencyResolver? DependencyResolver,
    IReadOnlyList<MiddlewareInstanceOrResolverFunc> Middlewares
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
            child.TimeoutCheckFrequency ?? TimeoutCheckFrequency,
            child.SuspensionCheckFrequency ?? SuspensionCheckFrequency,
            child.EventSourcePullFrequency ?? EventSourcePullFrequency,
            child.DelayStartup ?? DelayStartup,
            child.MaxParallelRetryInvocations ?? MaxParallelRetryInvocations,
            child.Serializer ?? Serializer,
            child.DependencyResolver ?? DependencyResolver,
            child.Middlewares.Any() ? child.Middlewares : Middlewares
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
            Serializer: DefaultSerializer.Instance,
            DependencyResolver: null,
            Middlewares: new List<MiddlewareInstanceOrResolverFunc>()
        );
}