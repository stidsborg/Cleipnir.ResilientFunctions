using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public interface IMiddleware
{
    Task<Result> InvokeAction<TParam>(
        TParam param,
        Context context,
        Func<TParam, Context, Task<Result>> next
    ) where TParam : notnull;
        
    Task<Result> InvokeAction<TParam, TScrapbook>(
        TParam param,
        TScrapbook scrapbook,
        Context context,
        Func<TParam, TScrapbook, Context, Task<Result>> next 
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
        
    Task<Result<TResult>> InvokeFunc<TParam, TResult>(
        TParam param,
        Context context,
        Func<TParam, Context, Task<Result<TResult>>> next 
    ) where TParam : notnull;
        
    Task<Result<TResult>> InvokeFunc<TParam, TScrapbook, TResult>(
        TParam param,
        TScrapbook scrapbook,
        Context context,
        Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next 
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}