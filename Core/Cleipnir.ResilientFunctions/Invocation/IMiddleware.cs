using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public interface IMiddleware
{
    Task<Result> InvokeAction<TParam>(
        FunctionId functionId,
        TParam param,
        Func<TParam, Task<Result>> inner
    ) where TParam : notnull;
        
    Task<Result> InvokeAction<TParam, TScrapbook>(
        FunctionId functionId,
        TParam param,
        TScrapbook scrapbook,
        Func<TParam, TScrapbook, Task<Result>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook;
        
    Task<Result<TResult>> InvokeFunc<TParam, TResult>(
        FunctionId functionId,
        TParam param,
        Func<TParam, Task<Result<TResult>>> inner
    ) where TParam : notnull;
        
    Task<Result<TResult>> InvokeFunc<TParam, TScrapbook, TResult>(
        FunctionId functionId,
        TParam param,
        TScrapbook scrapbook,
        Func<TParam, TScrapbook, Task<Result<TResult>>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook;
}