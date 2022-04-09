using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public static class CommonAdapters
{
    public static Func<TParam, Task<Result>> ToInnerAction<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return param =>
        {
            inner(param);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> ToInnerAction<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook) =>
        {
            await inner(param, scrapbook);
            return Result.Succeed;
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> ToInnerAction<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            inner(param, scrapbook);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TParam, Task<Result>> ToInnerAction<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TParam, Task<Result>> ToInnerAction<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async param =>
        {
            await inner(param);
            return Result.Succeed;
        };
    }
    
    public static Func<Metadata<TParam>, Task> ToPreInvoke<TParam>(
        Action<Metadata<TParam>> preInvoke
    ) where TParam : notnull
    {
        return metadata =>
        {
            preInvoke(metadata);
            return Task.CompletedTask;
        };
    }
    
    public static Func<TScrapbook, Metadata<TParam>, Task> ToPreInvoke<TParam, TScrapbook>(
        Action<TScrapbook, Metadata<TParam>> preInvoke
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (scrapbook, metadata) =>
        {
            preInvoke(scrapbook, metadata);
            return Task.CompletedTask;
        };
    }

    public static Func<Metadata<TParam>, Task> NoOpPreInvoke<TParam>() where TParam : notnull
    {
        return _ => Task.CompletedTask;
    }
    
    public static Func<TScrapbook, Metadata<TParam>, Task> NoOpPreInvoke<TParam, TScrapbook>() 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (_,_) => Task.CompletedTask;
    }

    public static Func<Result, Metadata<TParam>, Task<Result>> ToPostInvoke<TParam>(
        Func<Result, Metadata<TParam>, Result> postInvoke
    ) where TParam : notnull
    {
        return (result, metadata) =>
        {
            result = postInvoke(result, metadata);
            return Task.FromResult(result);
        };
    }
    
    public static Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> ToPostInvoke<TParam, TScrapbook>(
        Func<Result, TScrapbook, Metadata<TParam>, Result> postInvoke
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (result, scrapbook, metadata) =>
        {
            result = postInvoke(result, scrapbook, metadata);
            return Task.FromResult(result);
        };
    }

    public static Func<Result, Metadata<TParam>, Task<Result>> NoOpPostInvoke<TParam>() where TParam : notnull
    {
        return (result, _) => Task.FromResult(result);
    }
    
    public static Func<Result, TScrapbook, Metadata<TParam>, Task<Result>> NoOpPostInvoke<TParam, TScrapbook>() 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (result, _, _) => Task.FromResult(result);
    }
}