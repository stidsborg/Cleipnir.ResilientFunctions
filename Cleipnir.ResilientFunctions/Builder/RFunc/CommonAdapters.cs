using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RFunc;

public static class CommonAdapters
{
    public static Func<TParam, Task<Return<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return param =>
        {
            var returned = inner(param);
            return Task.FromResult(new Return<TReturn>(returned));
        };
    }
    
    public static Func<TParam, Task<Return<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async param =>
        {
            var returned = await inner(param);
            return Succeed.WithValue(returned);
        };
    }
    
    public static Func<TParam, Task<Return<TReturn>>> ToInnerFunc<TParam, TReturn>(Func<TParam, Return<TReturn>> inner) where TParam : notnull
    {
        return param =>
        {
            var returned = inner(param);
            return Task.FromResult(returned);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Return<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var returned = inner(param, scrapbook);
            return Task.FromResult(new Return<TReturn>(returned));
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Return<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook) =>
        {
            var returned = await inner(param, scrapbook);
            return Succeed.WithValue(returned);
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Return<TReturn>>> ToInnerFunc<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Return<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook) =>
        {
            var returned = inner(param, scrapbook);
            return Task.FromResult(returned);
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

    public static Func<Return<TReturn>, Metadata<TParam>, Task<Return<TReturn>>> ToAsyncPostInvoke<TParam, TReturn>(
        Func<Return<TReturn>, Metadata<TParam>, Return<TReturn>> postInvoke
    ) where TParam : notnull
    {
        return (returned, metadata) =>
        {
            returned = postInvoke(returned, metadata);
            return Task.FromResult(returned);
        };
    }

    public static Func<Return<TReturn>, TScrapbook, Metadata<TParam>, Task<Return<TReturn>>> ToAsyncPostInvoke<TParam, TScrapbook, TReturn>(
        Func<Return<TReturn>, TScrapbook, Metadata<TParam>, Return<TReturn>> postInvoke
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (returned, scrapbook, metadata) =>
        {
            returned = postInvoke(returned, scrapbook, metadata);
            return Task.FromResult(returned);
        };
    }

    public static Func<Return<TReturn>, Metadata<TParam>, Task<Return<TReturn>>> NoOpPostInvoke<TParam, TReturn>() where TParam : notnull
    {
        return (returned, _) => Task.FromResult(returned);
    }
    
    public static Func<Return<TReturn>, TScrapbook, Metadata<TParam>, Task<Return<TReturn>>> NoOpPostInvoke<TParam, TScrapbook, TReturn>() 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (returned, _, _) => Task.FromResult(returned);
    }
}