using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record MiddlewareInstanceOrResolverFunc(
    IMiddleware? Instance,
    Func<IScopedDependencyResolver, IMiddleware>? Resolver
);