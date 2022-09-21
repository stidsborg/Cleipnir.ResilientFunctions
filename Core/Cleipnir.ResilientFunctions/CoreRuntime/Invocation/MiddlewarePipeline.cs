using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class MiddlewarePipeline
{
    private readonly IReadOnlyList<MiddlewareInstanceOrResolverFunc> _middlewares;
    
    public MiddlewarePipeline(IReadOnlyList<MiddlewareInstanceOrResolverFunc> middlewares) => _middlewares = middlewares;

    public Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> WrapPipelineAroundInner<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner, 
        IScopedDependencyResolver? scopedDependencyResolver,
        PreCreationParameters<TParam>? preCreationParameters
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var curr = inner;
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);

            if (middleware is IPreCreationMiddleware preInvokeMiddleware && preCreationParameters.HasValue)
                preInvokeMiddleware.PreCreation(
                    preCreationParameters.Value.Param, 
                    preCreationParameters.Value.StateDictionary, 
                    preCreationParameters.Value.FunctionId
                );

            var wrapper = new MiddlewareWrapper<TParam, TScrapbook, TReturn>(middleware!, curr);
            curr = wrapper.Invoke;
        }

        return curr;
    }

    private class MiddlewareWrapper<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        private readonly IMiddleware _middleware;
        private readonly Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> _next;
        public MiddlewareWrapper(IMiddleware middleware, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> next)
        {
            _middleware = middleware;
            _next = next;
        }

        [DebuggerStepThrough]
        public Task<Result<TReturn>> Invoke(
            TParam param,
            TScrapbook scrapbook,
            Context context
        ) 
        { 
            return _middleware.Invoke(param, scrapbook, context, _next);
        }
    }
}

public record struct PreCreationParameters<TParam>(
    TParam Param, 
    Dictionary<string, string> StateDictionary, 
    FunctionId FunctionId
);