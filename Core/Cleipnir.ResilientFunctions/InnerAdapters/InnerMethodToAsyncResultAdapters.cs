using System;
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
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** ASYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TReturn>(
        Func<TEntity, Func<TParam, Workflow, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, workflow);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! FUNC WITH STATE !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, state);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Workflow, TReturn>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                var result = inner(param, state, workflow);
                return Task.FromResult(new Result<TReturn>(result));
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, state);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Workflow, Task<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                var result = await inner(param, state, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, state);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Workflow, Result<TReturn>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, state, workflow);
                return Task.FromResult(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<TReturn>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<TReturn>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<TReturn>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<TReturn>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState, TReturn>(
        Func<TEntity, Func<TParam, TState, Task<Result<TReturn>>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, state);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Action<TParam, Workflow>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, workflow);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Workflow, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, workflow);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Workflow, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, workflow);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, WorkflowState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam>(
        Func<TEntity, Func<TParam, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** !! ACTION WITH STATE !! ** //
    // ** SYNC ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Action<TParam, TState>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, state);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Action<TParam, TState, Workflow>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity); 
                inner(param, state, workflow);
                return Task.FromResult(Result.Succeed.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** ASYNC ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Func<TParam, TState, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, state);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** ASYNC W. WORKFLOW * //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Func<TParam, TState, Workflow, Task>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);   
                await inner(param, state, workflow);
                return Result.Succeed.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
    
    // ** SYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Func<TParam, TState, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, state);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Func<TParam, TState, Workflow, Result>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = inner(param, state, workflow);
                return Task.FromResult(result.ToUnit());
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>().ToTask(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>().ToTask(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>().ToTask(); }
        };
    }

    // ** ASYNC W. RESULT ** //
    public static Func<TEntity, Func<TParam, TState, Workflow, Task<Result<Unit>>>> ToInnerFToInnerWithTaskResultReturnuncWithTaskResultReturn<TEntity, TParam, TState>(
        Func<TEntity, Func<TParam, TState, Task<Result>>> innerMethodSelector
    ) where TParam : notnull
    {
        return entity => async (param, state, workflow) =>
        {
            try
            {
                var inner = innerMethodSelector(entity);
                var result = await inner(param, state);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil).ToResult<Unit>(); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value).ToResult<Unit>(); }
            catch (Exception exception) { return Fail.WithException(exception).ToResult<Unit>(); }
        };
    }
}