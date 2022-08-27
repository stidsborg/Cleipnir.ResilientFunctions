using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public class MiddlewarePipeline
{
    private readonly IReadOnlyList<MiddlewareOrResolver> _middlewares;
    
    public MiddlewarePipeline(IReadOnlyList<MiddlewareOrResolver> middlewares) => _middlewares = middlewares;

    public Func<TParam, Task<Result>> WrapPipelineAroundInnerAction<TParam>(
        FunctionId functionId, 
        InvocationMode invocationMode,
        Func<TParam, Task<Result>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
            var middlewareAction = new MiddlewareAction<TParam>(functionId, curr, middleware!, invocationMode);    
            curr = middlewareAction.Invoke;
        }

        return curr;
    }

    private record MiddlewareAction<TParam> (FunctionId FunctionId, Func<TParam, Task<Result>> Next, IMiddleware Middleware, InvocationMode InvocationMode) 
        where TParam : notnull 
    {
        [DebuggerStepThrough]
        public Task<Result> Invoke(TParam param) => Middleware.InvokeAction(FunctionId, param, Next, InvocationMode);
    }
    
    public Func<TParam, TScrapbook, Task<Result>> WrapPipelineAroundInnerAction<TParam, TScrapbook>(
        FunctionId functionId, 
        InvocationMode invocationMode,
        Func<TParam, TScrapbook, Task<Result>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull where TScrapbook : RScrapbook
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
                
            curr = (param, scrapbook) => middleware!.InvokeAction(functionId, param, scrapbook, next, invocationMode);
        }

        return curr;
    }
    
    public Func<TParam, Task<Result<TReturn>>> WrapPipelineAroundInnerFunc<TParam, TReturn>(
        FunctionId functionId, 
        InvocationMode invocationMode,
        Func<TParam, Task<Result<TReturn>>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
                
            curr = param => middleware!.InvokeFunc(functionId, param, next, invocationMode);
        }

        return curr;
    }
    
    public Func<TParam, TScrapbook, Task<Result<TReturn>>> WrapPipelineAroundInnerFunc<TParam, TScrapbook, TReturn>(
        FunctionId functionId, 
        InvocationMode invocationMode,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver
    ) where TParam : notnull where TScrapbook : RScrapbook
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);
                
            curr = (param, scrapbook) => middleware!.InvokeFunc(functionId, param, scrapbook, next, invocationMode);
        }

        return curr;
    }
}