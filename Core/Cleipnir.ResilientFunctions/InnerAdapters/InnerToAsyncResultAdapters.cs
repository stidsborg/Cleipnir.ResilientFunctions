using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

internal static class InnerToAsyncResultAdapters
{
    // ** !! PARAMLESS !! ** //
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Task> inner)
    {
        return async (_, _) =>
        {
            try
            {
                await inner();
                return Unit.Instance;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Workflow, Task> inner)
    {
        return async (_, workflow) =>
        {
            try
            {
                await inner(workflow);
                return Unit.Instance;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Task<Result>> inner)
    {
        return async (_, _) =>
        {
            try
            {
                var result = await inner();
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Task<Result<Unit>>> inner)
    {
        return async (_, _) =>
        {
            try
            {
                var result = await inner();
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Workflow, Task<Result<Unit>>> inner)
    {
        return async (_, workflow) =>
        {
            try
            {
                var result = await inner(workflow);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Workflow, Task<Result>> inner)
    {
        return async (_, workflow) =>
        {
            try
            {
                var result = await inner(workflow);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! ACTION !! ** //
    // ** ASYNC ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                await inner(param);    
                return Result.Succeed.ToUnit();
            } 
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                await inner(param, workflow);
                return Result.Succeed.ToUnit();    
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. RESULT AND workflow ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task<Result>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return result.ToUnit();
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** !! FUNCTION !! ** //
    // ** ASYNC ** //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow * //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<TReturn>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return Succeed.WithValue(result);
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
    
    // ** ASYNC W. workflow AND RESULT * //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param, workflow);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
   
    // ** ASYNC W. RESULT ** //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
    {
        return async (param, workflow) =>
        {
            try
            {
                var result = await inner(param);
                return result;
            }
            catch (PostponeInvocationException exception) { return Postpone.Until(exception.PostponeUntil); }
            catch (SuspendInvocationException exception) { return Suspend.While(exception.ExpectedInterruptCount.Value); }
            catch (Exception exception) { return Fail.WithException(exception); }
        };
    }
}