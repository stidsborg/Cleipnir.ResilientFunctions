using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RAction;

public static class CommonAdapters
{
    public static Func<TParam, Task<Return>> ToInnerAction<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return param =>
        {
            inner(param);
            return Task.FromResult(Return.Succeed);
        };
    }
    
    public static Func<TParam, Task<Return>> ToInnerAction<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async param =>
        {
            await inner(param);
            return Return.Succeed;
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

    public static Func<Return, Metadata<TParam>, Task<Return>> ToPostInvoke<TParam>(
        Func<Return, Metadata<TParam>, Return> postInvoke
    ) where TParam : notnull
    {
        return (returned, metadata) =>
        {
            var postInvoked = postInvoke(returned, metadata);
            return Task.FromResult(postInvoked);
        };
    }

    public static Func<Return, Metadata<TParam>, Task<Return>> NoOpPostInvoke<TParam>() where TParam : notnull
    {
        return (returned, _) => Task.FromResult(returned);
    }
}