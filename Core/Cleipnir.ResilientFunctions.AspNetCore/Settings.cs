﻿using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public class Settings
{
    private readonly Action<RFunctionException>? _unhandledExceptionHandler;
    private readonly TimeSpan? _crashedCheckFrequency;
    private readonly TimeSpan? _postponedCheckFrequency;
    private readonly TimeSpan? _delayStartup;
    private readonly int? _maxParallelRetryInvocations;
    private readonly ISerializer? _serializer;
    private readonly List<MiddlewareInstanceOrResolverFunc> _middlewares = new();

    public Settings(
        Action<RFunctionException>? UnhandledExceptionHandler = null, 
        TimeSpan? CrashedCheckFrequency = null, 
        TimeSpan? PostponedCheckFrequency = null, 
        TimeSpan? DelayStartup = null, 
        int? MaxParallelRetryInvocations = null, 
        ISerializer? Serializer = null
    )
    {
        _unhandledExceptionHandler = UnhandledExceptionHandler;
        _crashedCheckFrequency = CrashedCheckFrequency;
        _postponedCheckFrequency = PostponedCheckFrequency;
        _delayStartup = DelayStartup;
        _maxParallelRetryInvocations = MaxParallelRetryInvocations;
        _serializer = Serializer;
    }

    public Settings UseMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
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

    internal Cleipnir.ResilientFunctions.Settings MapToRFunctionsSettings(IDependencyResolver dependencyResolver)
        => new(
            _unhandledExceptionHandler,
            _crashedCheckFrequency,
            _postponedCheckFrequency,
            _delayStartup,
            _maxParallelRetryInvocations,
            _serializer,
            dependencyResolver,
            _middlewares
        );
}