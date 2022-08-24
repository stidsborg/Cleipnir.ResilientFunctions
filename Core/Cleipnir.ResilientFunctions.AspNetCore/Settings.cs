using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public class Settings
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? CrashedCheckFrequency { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal ISerializer? Serializer { get; }

    private readonly List<MiddlewareOrResolver> _middlewares = new();
    internal IEnumerable<MiddlewareOrResolver> Middlewares => _middlewares;

    public Settings(
        Action<RFunctionException>? UnhandledExceptionHandler = null, 
        TimeSpan? CrashedCheckFrequency = null, 
        TimeSpan? PostponedCheckFrequency = null, 
        TimeSpan? DelayStartup = null, 
        int? MaxParallelRetryInvocations = null, 
        ISerializer? Serializer = null
    )
    {
        this.UnhandledExceptionHandler = UnhandledExceptionHandler;
        this.CrashedCheckFrequency = CrashedCheckFrequency;
        this.PostponedCheckFrequency = PostponedCheckFrequency;
        this.DelayStartup = DelayStartup;
        this.MaxParallelRetryInvocations = MaxParallelRetryInvocations;
        this.Serializer = Serializer;
    }

    public Settings RegisterMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
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

    internal Cleipnir.ResilientFunctions.Settings MapToRFunctionsSettings(IDependencyResolver dependencyResolver)
        => new(
            UnhandledExceptionHandler,
            CrashedCheckFrequency,
            PostponedCheckFrequency,
            DelayStartup,
            MaxParallelRetryInvocations,
            Serializer,
            dependencyResolver
        );
}