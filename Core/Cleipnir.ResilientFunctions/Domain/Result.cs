using System;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public static class Result
{
    public static Result<T> SucceedWithValue<T>(T value) => new(value);
    public static Result<Unit> SucceedWithUnit { get; } = new Result<Unit>(Unit.Instance);
}

public static class Succeed
{
    public static Result<T> WithValue<T>(T value) => new Result<T>(value);
    public static Result<Unit> WithUnit { get; } = new Result<Unit>(Unit.Instance);
}

public class Fail
{
    public FatalWorkflowException Exception { get; } 
    
    public Fail(FatalWorkflowException exception) => Exception = exception;

    public Fail(FlowId flowId, FatalWorkflowException exception)
    {
        exception.FlowId = flowId;
        Exception = exception;
    }
    
    public Fail(Exception exception) => 
        Exception = exception as FatalWorkflowException ?? FatalWorkflowException.CreateNonGeneric(null!, exception);

    public static Fail WithException(Exception exception) => new Fail(exception);
    public static Fail WithException(FatalWorkflowException exception, FlowId flowId) => new Fail(flowId, exception);

    public Result<T> ToResult<T>() => new(Exception);
}

public class Postpone
{
    public DateTime DateTime { get; }

    private Postpone(DateTime dateTime) => DateTime = dateTime;

    public Result<T> ToResult<T>() => new(this);
    public Result<Unit> ToUnitResult => ToResult<Unit>();
    
    public static Postpone Until(DateTime dateTime) => new(dateTime.ToUniversalTime());
    public static void Throw(DateTime postponeUntil) => throw new PostponeInvocationException(postponeUntil);
}

public class Suspend
{
    public static Suspend Invocation { get; } = new();
    
    public Result<T> ToResult<T>() => new Result<T>(this);
}

public class Result<T>
{
    public Outcome Outcome { get; }
    public bool Succeed { get; }
    public T? SucceedWithValue { get; }
    public Postpone? Postpone { get; }
    public FatalWorkflowException? Fail { get; }
    public Suspend? Suspend { get; }

    public Result(T succeedWithValue) : this(succeed: true, succeedWithValue, postpone: null, fail: null, suspend: null) {}
    public Result(Postpone postpone) : this(succeed: false, succeedWithValue: default, postpone, fail: null, suspend: null) {}
    public Result(FatalWorkflowException failWith) : this(succeed: false, succeedWithValue: default, postpone: null, fail: failWith, suspend: null) {}
    public Result(Suspend suspend) : this(succeed: false, succeedWithValue: default, postpone: null, fail: null, suspend) {}

    private Result(bool succeed, T? succeedWithValue, Postpone? postpone, FatalWorkflowException? fail, Suspend? suspend)
    {
        if (succeed)
            Outcome = Outcome.Succeed;
        else if (postpone != null)
            Outcome = Outcome.Postpone;
        else if (fail != null)
            Outcome = Outcome.Fail;
        else
            Outcome = Outcome.Suspend;
        
        Succeed = succeed;
        SucceedWithValue = succeedWithValue;
        Postpone = postpone;
        Fail = fail;
        Suspend = suspend;
    }

    public static implicit operator Result<T>(T succeedWithValue) => new Result<T>(succeedWithValue);
    public static implicit operator Result<T>(Fail fail) => new Result<T>(fail.Exception);
    public static implicit operator Result<T>(Postpone postpone) => new Result<T>(postpone);
    public static implicit operator Result<T>(Suspend suspend) => new Result<T>(suspend);
}

public enum Outcome
{
    Succeed = 1,
    Postpone = 2,
    Fail = 3,
    Suspend = 4
}
