using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

internal static class InnerToAsyncResultAdapters
{
    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                inner(param);
                return Result.Succeed.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam, Context> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                inner(param, context);
                return Result.Succeed.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                await inner(param);    
                return Result.Succeed.ToUnit();
            } 
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                await inner(param, context);
                return Result.Succeed.ToUnit();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param);
                return result.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, context);
                return result.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Context, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, context);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                inner(param, scrapbook);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook, Context> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
            {
                try
                {
                    inner(param, scrapbook, context);
                    return Task.FromResult(Result.Succeed.ToUnit());
                }
                catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
                catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
                catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
            };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                await inner(param, scrapbook);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Context, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                await inner(param, scrapbook, context);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND CONTEXT ** //  
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Context, Task<Result>> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, scrapbook, context);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, scrapbook);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Context, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, scrapbook, context);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task<Result>> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** !! FUNCTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, context);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, context);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. CONTEXT AND RESULT * //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, context);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Context, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, context);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
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
            try
            {
                var result = inner(param, scrapbook);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, scrapbook, context);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. CONTEXT * //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, scrapbook, context);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, scrapbook);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Context, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, context) =>
        {
            try
            {
                var result = inner(param, scrapbook, context);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, context) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.Until(exception.SuspendUntilEventSourceCountAtLeast); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
}