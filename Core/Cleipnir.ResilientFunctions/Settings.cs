using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? CrashedCheckFrequency { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal ISerializer? Serializer { get; }
    internal IDependencyResolver? DependencyResolver { get; }
    
    private readonly List<MiddlewareOrResolver> _middlewares = new();
    internal IReadOnlyList<MiddlewareOrResolver> Middlewares => _middlewares;

    public Settings(
        Action<RFunctionException>? UnhandledExceptionHandler = null, 
        TimeSpan? CrashedCheckFrequency = null, 
        TimeSpan? PostponedCheckFrequency = null, 
        TimeSpan? DelayStartup = null, 
        int? MaxParallelRetryInvocations = null, 
        ISerializer? Serializer = null, 
        IDependencyResolver? DependencyResolver = null
    ) :this(
        UnhandledExceptionHandler, CrashedCheckFrequency, PostponedCheckFrequency, DelayStartup, 
        MaxParallelRetryInvocations, Serializer, DependencyResolver, 
        middlewares: new List<MiddlewareOrResolver>()
    ) { }

    internal Settings(
        Action<RFunctionException>? unhandledExceptionHandler, 
        TimeSpan? crashedCheckFrequency, 
        TimeSpan? postponedCheckFrequency, 
        TimeSpan? delayStartup, 
        int? maxParallelRetryInvocations, 
        ISerializer? serializer, 
        IDependencyResolver? dependencyResolver,
        List<MiddlewareOrResolver> middlewares
    )
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        CrashedCheckFrequency = crashedCheckFrequency;
        PostponedCheckFrequency = postponedCheckFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
        DependencyResolver = dependencyResolver;
        _middlewares = middlewares;
    }

    public Settings RegisterMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
        if (DependencyResolver == null)
            throw new InvalidOperationException(
                $"{DependencyResolver} must be non-null when registering middleware using generic argument"
            );

        _middlewares.Add(
            new MiddlewareOrResolver(
                Middleware: null,
                MiddlewareResolver: resolver => resolver.Resolve<TMiddleware>()
            )
        );

        return this;
    }

    public Settings RegisterMiddleware(IMiddleware middleware) 
    {
        _middlewares.Add(new MiddlewareOrResolver(middleware, MiddlewareResolver: null));
        return this;
    }
}

public record SettingsWithDefaults(
    UnhandledExceptionHandler UnhandledExceptionHandler,
    TimeSpan CrashedCheckFrequency,
    TimeSpan PostponedCheckFrequency,
    TimeSpan DelayStartup,
    int MaxParallelRetryInvocations,
    ISerializer Serializer,
    IDependencyResolver? DependencyResolver,
    IReadOnlyList<MiddlewareOrResolver> Middlewares
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
            DelayStartup: TimeSpan.FromSeconds(0),
            MaxParallelRetryInvocations: 10,
            Serializer: DefaultSerializer.Instance,
            DependencyResolver: null,
            Middlewares: new List<MiddlewareOrResolver>()
        );
}