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
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                inner(param);
                return Result.Succeed.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** SYNC W. workflow ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam, Workflow> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                inner(param, workflow);
                return Result.Succeed.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                await inner(param);    
                return Result.Succeed.ToUnit();
            } 
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                await inner(param, workflow);
                return Result.Succeed.ToUnit();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param);
                return result.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Result> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, workflow);
                return result.ToUnit().ToTask();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND workflow ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task<Result>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                inner(param, scrapbook);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. workflow ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Action<TParam, TScrapbook, Workflow> inner) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
            {
                try
                {
                    inner(param, scrapbook, workflow);
                    return Task.FromResult(Result.Succeed.ToUnit());
                }
                catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
                catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
                catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
            };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                await inner(param, scrapbook);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Workflow, Task> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                await inner(param, scrapbook, workflow);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND workflow ** //  
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Workflow, Task<Result>> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, scrapbook, workflow);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Workflow, Result> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook, workflow);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TScrapbook>(Func<TParam, TScrapbook, Task<Result>> inner)
        where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** !! FUNCTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. workflow ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, TReturn> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow AND RESULT * //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** FUNCTION WITH SCRAPBOOK ** //
    // ** SYNC ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. workflow ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Workflow, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Workflow, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, scrapbook, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Workflow, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return (param, scrapbook, workflow) =>
        {
            try
            {
                var result = inner(param, scrapbook, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        return async (param, scrapbook, workflow) =>
        {
            try
            {
                var result = await inner(param, scrapbook);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
}