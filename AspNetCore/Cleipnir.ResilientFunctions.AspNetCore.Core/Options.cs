using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.AspNetCore.Core;

public class Options
{
    private readonly Action<RFunctionException>? _unhandledExceptionHandler;
    private readonly TimeSpan? _crashedCheckFrequency;
    private readonly TimeSpan? _postponedCheckFrequency;
    private readonly TimeSpan? _delayStartup;
    private readonly int? _maxParallelRetryInvocations;
    private readonly ISerializer? _serializer;
    private readonly List<MiddlewareInstanceOrResolverFunc> _middlewares = new();

    public Options(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? crashedCheckFrequency = null, 
        TimeSpan? postponedCheckFrequency = null, 
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null
    )
    {
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _crashedCheckFrequency = crashedCheckFrequency;
        _postponedCheckFrequency = postponedCheckFrequency;
        _delayStartup = delayStartup;
        _maxParallelRetryInvocations = maxParallelRetryInvocations;
        _serializer = serializer;
    }

    public Options UseMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
        _middlewares.Add(
            new MiddlewareInstanceOrResolverFunc(
                Instance: null,
                Resolver: resolver => resolver.Resolve<TMiddleware>()
            )
        );

        return this;
    }

    public Options UseMiddleware(IMiddleware middleware) 
    {
        _middlewares.Add(new MiddlewareInstanceOrResolverFunc(middleware, Resolver: null));
        return this;
    }

    internal Settings MapToRFunctionsSettings(IDependencyResolver dependencyResolver)
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