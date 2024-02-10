﻿using System;
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Action<TParam, Workflow> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Result> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Result> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task<Result>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    
    // ** !! ACTION WITH STATE !! ** //
    // ** SYNC ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Action<TParam, TState> inner) 
        where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                inner(param, state);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. workflow ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Action<TParam, TState, Workflow> inner) 
        where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
            {
                try
                {
                    inner(param, state, workflow);
                    return Task.FromResult(Result.Succeed.ToUnit());
                }
                catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
                catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
                catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
            };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Task> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                await inner(param, state);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Workflow, Task> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                await inner(param, state, workflow);
                return Result.Succeed.ToUnit();  
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND workflow ** //  
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Workflow, Task<Result>> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                var result = await inner(param, state, workflow);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Result> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Workflow, Result> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state, workflow);
                return result.ToUnit().ToTask();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TState, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam, TState>(Func<TParam, TState, Task<Result>> inner)
        where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                var result = await inner(param, state);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** !! FUNCTION !! ** //
    // ** SYNC ** //
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, TReturn> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, TReturn> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Result<TReturn>> inner) where TParam : notnull
    {
        return (param, state, workflow) =>
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
    public static Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, state, workflow) =>
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
    
    // ** FUNCTION WITH STATE ** //
    // ** SYNC ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, TReturn> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. workflow ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Workflow, TReturn> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Task<TReturn>> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                var result = await inner(param, state);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Workflow, Task<TReturn>> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                var result = await inner(param, state, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    
    // ** SYNC W. RESULT ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Result<TReturn>> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND workflow ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Workflow, Result<TReturn>> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return (param, state, workflow) =>
        {
            try
            {
                var result = inner(param, state, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, TState, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TState, TReturn>(
        Func<TParam, TState, Task<Result<TReturn>>> inner
    ) where TParam : notnull where TState : WorkflowState, new()
    {
        return async (param, state, workflow) =>
        {
            try
            {
                var result = await inner(param, state);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.UntilAfter(exception.ExpectedEventCount); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
}