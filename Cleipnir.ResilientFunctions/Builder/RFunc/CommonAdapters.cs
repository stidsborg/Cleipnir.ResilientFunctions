using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RFunc;

public static class CommonAdapters
{
    public static Func<TParam, Task<Result<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async param =>
        {
            var result = await inner(param);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook) =>
        {
            var result = await inner(param, scrapbook);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(result);
        };
    }

    public static Func<TScrapbook, Metadata<TParam>, Task> ToAsyncPreInvoke<TParam, TScrapbook>(Action<TScrapbook, Metadata<TParam>> preInvoke)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (scrapbook, metadata) =>
        {
            preInvoke(scrapbook, metadata);
            return Task.CompletedTask;
        };
    } 

    public static Func<Metadata<TParam>, Task> ToAsyncPreInvoke<TParam>(Action<Metadata<TParam>> preInvoke)
        where TParam : notnull
    {
        return metadata =>
        {
            preInvoke(metadata);
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

    public static Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>> ToAsyncPostInvoke<TParam, TReturn>(
        Func<Result<TReturn>, Metadata<TParam>, Result<TReturn>> postInvoke
    ) where TParam : notnull
    {
        return (result, metadata) =>
        {
            result = postInvoke(result, metadata);
            return Task.FromResult(result);
        };
    }

    public static Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>> ToAsyncPostInvoke<TParam, TScrapbook, TReturn>(
        Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Result<TReturn>> postInvoke
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (result, scrapbook, metadata) =>
        {
            result = postInvoke(result, scrapbook, metadata);
            return Task.FromResult(result);
        };
    }

    public static Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>> NoOpPostInvoke<TParam, TReturn>() where TParam : notnull
    {
        return (result, _) => Task.FromResult(result);
    }
    
    public static Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>> NoOpPostInvoke<TParam, TScrapbook, TReturn>() 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (result, _, _) => Task.FromResult(result);
    }
}