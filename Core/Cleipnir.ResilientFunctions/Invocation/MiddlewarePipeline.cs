using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

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
            var next = curr;
            var (middleware, resolver) = _middlewares[i];
            if (resolver != null)
                middleware = resolver(scopedDependencyResolver!);

            if (middleware is IPreCreationMiddleware preInvokeMiddleware && preCreationParameters.HasValue)
                preInvokeMiddleware.PreCreation(
                    preCreationParameters.Value.Param, 
                    preCreationParameters.Value.StateDictionary, 
                    preCreationParameters.Value.FunctionId
                );
            
            curr = (param, scrapbook, context) => middleware!.Invoke(param, scrapbook, context, next);
        }

        return curr;
    }
}

public record struct PreCreationParameters<TParam>(
    TParam Param, 
    Dictionary<string, string> StateDictionary, 
    FunctionId FunctionId
);