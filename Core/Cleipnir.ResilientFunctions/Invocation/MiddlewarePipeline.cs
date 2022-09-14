using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public class MiddlewarePipeline
{
    private readonly IReadOnlyList<MiddlewareOrResolver> _middlewares;
    
    public MiddlewarePipeline(IReadOnlyList<MiddlewareOrResolver> middlewares) => _middlewares = middlewares;
    
    public Func<TParam, TScrapbook, Context, Task<Result>> WrapPipelineAroundInnerAction<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Context, Task<Result>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
                
            curr = (param, scrapbook, context) => middleware!.InvokeAction(param, scrapbook, context, next);
        }

        return curr;
    }

    public Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> WrapPipelineAroundInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
                
            curr = (param, scrapbook, context) => middleware!.InvokeFunc(param, scrapbook, context, next);
        }

        return curr;
    }
}