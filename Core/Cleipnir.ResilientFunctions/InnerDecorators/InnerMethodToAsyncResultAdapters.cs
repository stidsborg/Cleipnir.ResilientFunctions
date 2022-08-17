using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerDecorators;

public static class InnerMethodToAsyncResultAdapters
{
    public static Func<TEntity, Func<TParam, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(Func<TEntity, Func<TParam, TReturn>> innerMethodSelector) where TParam : notnull
    {
        return entity => param =>
        {
            var inner = innerMethodSelector(entity); 
            var result = inner(param);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TEntity, Func<TParam, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(Func<TEntity, Func<TParam, Task<TReturn>>> innerMethodSelector) where TParam : notnull
    {
        return entity => async param =>
        {
            var inner = innerMethodSelector(entity);   
            var result = await inner(param);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TEntity, Func<TParam, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(Func<TEntity, Func<TParam, Result<TReturn>>> innerMethodSelector) where TParam : notnull
    {
        return entity => param =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TEntity, Func<TParam, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam>(Func<TEntity, Action<TParam>> innerMethodSelector) where TParam : notnull
    {
        return entity => param =>
        {
            var inner = innerMethodSelector(entity);
            inner(param);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(Func<TEntity, Func<TParam, TScrapbook, Task>> innerMethodSelector)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => async (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            await inner(param, scrapbook);
            return Result.Succeed;
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(Func<TEntity, Func<TParam, TScrapbook, Result>> innerMethodSelector)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook);
            return result.ToTask();
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(Func<TEntity, Action<TParam, TScrapbook>> innerMethodSelector) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            inner(param, scrapbook);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    public static Func<TEntity, Func<TParam, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam>(Func<TEntity, Func<TParam, Result>> innerMethodSelector) where TParam : notnull
    {
        return entity => param =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    public static Func<TEntity, Func<TParam, Task<Result>>> ToInnerWithTaskResultReturn<TEntity, TParam>(Func<TEntity, Func<TParam, Task>> innerMethodSelector) where TParam : notnull
    {
        return entity => async param =>
        {
            var inner = innerMethodSelector(entity);
            await inner(param);
            return Result.Succeed;
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> innerMethodSelector
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => async (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param, scrapbook);
            return Succeed.WithValue(result);
        };
    }
    
    public static Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return entity => (param, scrapbook) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook);
            return Task.FromResult(result);
        };
    }
}