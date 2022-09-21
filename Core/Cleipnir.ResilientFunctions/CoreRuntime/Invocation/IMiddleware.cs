using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public interface IMiddleware
{
    Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
        TParam param,
        TScrapbook scrapbook,
        Context context,
        Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next 
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public interface IPreCreationMiddleware : IMiddleware
{
    Task PreCreation<TParam>(
        TParam param,
        Dictionary<string, string> stateDictionary,
        FunctionId functionId
    ) where TParam : notnull;
}