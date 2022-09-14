using System;

namespace Cleipnir.ResilientFunctions.Invocation;

public record MiddlewareInstanceOrResolverFunc(
    IMiddleware? Instance,
    Func<IScopedDependencyResolver, IMiddleware>? Resolver
);