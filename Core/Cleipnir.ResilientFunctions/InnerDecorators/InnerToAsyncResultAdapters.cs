using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Invocation;

namespace Cleipnir.ResilientFunctions.InnerDecorators;

public static class InnerToAsyncResultAdapters
{
    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            inner(param);
            return Task.FromResult(Result.Succeed);
        };
    }

    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam, Context> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            inner(param, context);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            await inner(param);
            return Result.Succeed;
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            await inner(param, context);
            return Result.Succeed;
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, context);
            return Task.FromResult(result);
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param);
            return result;
        };
    }
    
    // ** ASYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, context);
            return result;
        };
    }
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            inner(param, scrapbook);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook, Context> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            inner(param, scrapbook, context);
            return Task.FromResult(Result.Succeed);
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            await inner(param, scrapbook);
            return Result.Succeed;
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Context, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            await inner(param, scrapbook, context);
            return Result.Succeed;
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook);
            return result.ToTask();
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Context, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook, context);
            return result.ToTask();
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task<Result>> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) 
            => await inner(param, scrapbook);
    }

    // ** !! FUNCTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, context);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param);
            return Succeed.WithValue(result);
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, context);
            return Succeed.WithValue(result);
        };
    }
    
    // ** ASYNC W. CONTEXT AND RESULT * //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, context);
            return result;
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, context);
            return Task.FromResult(result);
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param);
            return result;
        };
    }
    
    // ** FUNCTION WITH SCRAPBOOK ** //
    // ** SYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook, context);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, scrapbook);
            return Succeed.WithValue(result);
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, scrapbook, context);
            return Succeed.WithValue(result);
        };
    }

    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook);
            return Task.FromResult(result);
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            var result = inner(param, scrapbook, context);
            return Task.FromResult(result);
        };
    }
    
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            var result = await inner(param, scrapbook);
            return result;
        };
    }
}