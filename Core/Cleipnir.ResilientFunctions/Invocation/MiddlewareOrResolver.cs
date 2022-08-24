using System;

namespace Cleipnir.ResilientFunctions.Invocation;

public record MiddlewareOrResolver(
    IMiddleware? Middleware,
    Func<IScopedDependencyResolver, IMiddleware>? MiddlewareResolver
);