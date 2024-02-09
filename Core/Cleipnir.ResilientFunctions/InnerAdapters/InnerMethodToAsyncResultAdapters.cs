﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

internal static class InnerMethodToAsyncResultAdapters
{
    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** ASYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, workflow);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! FUNC WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, scrapbook);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Workflow, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, scrapbook, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, scrapbook);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, scrapbook, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, scrapbook);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Workflow, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, scrapbook, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook, TReturn>(
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, scrapbook);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam, Workflow>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, workflow);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Workflow, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, workflow);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Workflow, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, workflow);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, RScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Action<TParam, TScrapbook>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, scrapbook);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Action<TParam, TScrapbook, Workflow>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, scrapbook, workflow);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, scrapbook);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Workflow, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, scrapbook, workflow);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, scrapbook);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Workflow, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, scrapbook, workflow);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TScrapbook, Workflow, Task<Result<Unit>>>> ToInnerFToInnerWithTaskResultReturnuncWithTaskResultReturn<TEntity, TParam, TScrapbook>(
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, scrapbook, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, scrapbook);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
}