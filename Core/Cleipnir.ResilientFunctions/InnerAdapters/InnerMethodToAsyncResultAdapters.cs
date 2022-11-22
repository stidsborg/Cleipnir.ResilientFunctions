using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

internal static class InnerMethodToAsyncResultAdapters
{
    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            var result = inner(param);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Context, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            var result = inner(param, context);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            var result = await inner(param);
            return Succeed.WithValue(result);
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Context, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            var result = await inner(param, context);
            return Succeed.WithValue(result);
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param);
            return Task.FromResult(result);
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Context, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, context);
            return Task.FromResult(result);
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param);
            return result;
        };
    }

    // ** ASYNC W. RESULT AND CONTEXT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Context, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param, context);
            return result;
        };
    }
    
    // ** !! FUNC WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            var result = inner(param, scrapbook);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Context, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            var result = inner(param, scrapbook, context);
            return Task.FromResult(new Result<TReturn>(result));
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            var result = await inner(param, scrapbook);
            return Succeed.WithValue(result);
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            var result = await inner(param, scrapbook, context);
            return Succeed.WithValue(result);
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook);
            return Task.FromResult(result);
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Context, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook, context);
            return Task.FromResult(result);
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param, scrapbook);
            return result;
        };
    }

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            inner(param);
            return Task.FromResult(Result.Succeed.ToUnit());
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam, Context>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            inner(param, context);
            return Task.FromResult(Result.Succeed.ToUnit());
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            await inner(param);
            return Result.Succeed.ToUnit();
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Context, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            await inner(param, context);
            return Result.Succeed.ToUnit();
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param);
            return Task.FromResult(result.ToUnit());
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Context, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, context);
            return Task.FromResult(result.ToUnit());
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param);
            return result.ToUnit();
        };
    }
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Action<TParam, TScrapbook>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            inner(param, scrapbook);
            return Task.FromResult(Result.Succeed.ToUnit());
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Action<TParam, TScrapbook, Context>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity); 
            inner(param, scrapbook, context);
            return Task.FromResult(Result.Succeed.ToUnit());
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            await inner(param, scrapbook);
            return Result.Succeed.ToUnit();
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Context, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);   
            await inner(param, scrapbook, context);
            return Result.Succeed.ToUnit();
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook);
            return Task.FromResult(result.ToUnit());
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Context, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = inner(param, scrapbook, context);
            return Task.FromResult(result.ToUnit());
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> ToInnerFToInnerWithTaskResultReturnuncWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, context) =>
        {
            var inner = innerMethodSelector(entity);
            var result = await inner(param, scrapbook);
            return result.ToUnit();
        };
    }
}