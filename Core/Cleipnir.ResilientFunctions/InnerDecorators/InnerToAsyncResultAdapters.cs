using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerDecorators;

public static class InnerToAsyncResultAdapters
{
    public static Func<TParam, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async param =>
        {
            var result = await inner(param);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TParam, Task<Result>> ToInnerWithTaskResultReturn<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return param =>
        {
            inner(param);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> ToInnerWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook) =>
        {
            await inner(param, scrapbook);
            return Result.Succeed;
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> ToInnerWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var result = inner(param, scrapbook);
            return result.ToTask();
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> ToInnerWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            inner(param, scrapbook);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TParam, Task<Result>> ToInnerWithTaskResultReturn<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return param =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TParam, Task<Result>> ToInnerWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async param =>
        {
            await inner(param);
            return Result.Succeed;
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook) =>
        {
            var result = await inner(param, scrapbook);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> ToInnerWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(result);
        };
    }
}