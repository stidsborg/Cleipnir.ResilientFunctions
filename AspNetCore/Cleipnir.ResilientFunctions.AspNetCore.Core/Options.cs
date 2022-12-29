﻿using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.AspNetCore.Core;

public class Options
{
    internal Action<RFunctionException>? UnhandledExceptionHandler { get; }
    internal TimeSpan? CrashedCheckFrequency { get; }
    internal TimeSpan? PostponedCheckFrequency { get; }
    internal TimeSpan? TimeoutCheckFrequency { get; }
    internal TimeSpan? SuspensionCheckFrequency { get; }
    internal TimeSpan? EventSourcePullFrequency { get; }
    internal TimeSpan? DelayStartup { get; }
    internal int? MaxParallelRetryInvocations { get; }
    internal ISerializer? Serializer { get; }
    internal List<MiddlewareInstanceOrResolverFunc> Middlewares  { get; } = new();

    public Options(
        Action<RFunctionException>? unhandledExceptionHandler = null, 
        TimeSpan? crashedCheckFrequency = null, 
        TimeSpan? postponedCheckFrequency = null, 
        TimeSpan? timeoutCheckFrequency = null,
        TimeSpan? suspensionCheckFrequency = null,
        TimeSpan? eventSourcePullFrequency = null,
        TimeSpan? delayStartup = null, 
        int? maxParallelRetryInvocations = null, 
        ISerializer? serializer = null
    )
    {
        UnhandledExceptionHandler = unhandledExceptionHandler;
        CrashedCheckFrequency = crashedCheckFrequency;
        PostponedCheckFrequency = postponedCheckFrequency;
        TimeoutCheckFrequency = timeoutCheckFrequency;
        SuspensionCheckFrequency = suspensionCheckFrequency;
        EventSourcePullFrequency = eventSourcePullFrequency;
        DelayStartup = delayStartup;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        Serializer = serializer;
    }

    public Options UseMiddleware<TMiddleware>() where TMiddleware : IMiddleware 
    {
        Middlewares.Add(
            new MiddlewareInstanceOrResolverFunc(
                Instance: null,
                Resolver: resolver => resolver.Resolve<TMiddleware>()
            )
        );

        return this;
    }

    public Options UseMiddleware(IMiddleware middleware) 
    {
        Middlewares.Add(new MiddlewareInstanceOrResolverFunc(middleware, Resolver: null));
        return this;
    }

    internal Settings MapToRFunctionsSettings(IDependencyResolver dependencyResolver)
        => new(
            UnhandledExceptionHandler,
            CrashedCheckFrequency,
            PostponedCheckFrequency,
            TimeoutCheckFrequency,
            SuspensionCheckFrequency,
            EventSourcePullFrequency,
            DelayStartup,
            MaxParallelRetryInvocations,
            Serializer,
            dependencyResolver,
            Middlewares
        );
}