using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.InnerDecorators;

public static class InnerToAsyncResultAdapters
{
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
}