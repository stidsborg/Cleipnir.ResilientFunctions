using System;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public class Result
{
    public Outcome Outcome { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }
    public Suspend? Suspend { get; }

    public Result() : this(succeed: true, postpone: null, fail: null, suspend: null) {}
    public Result(Postpone postpone) : this(succeed: false, postpone, fail: null, suspend: null) {}
    public Result(Exception exception) : this(succeed: false, postpone: null, fail: exception, suspend: null) {}
    public Result(Suspend suspend) : this(succeed: false, postpone: null, fail: null, suspend) {}

    private Result(bool succeed, Postpone? postpone, Exception? fail, Suspend? suspend)
    {
        if (succeed)
            Outcome = Outcome.Succeed;
        else if (postpone != null)
            Outcome = Outcome.Postpone;
        else if (fail != null)
            Outcome = Outcome.Fail;
        else
            Outcome = Outcome.Suspend;
        
        Postpone = postpone;
        Fail = fail;
        Postpone = postpone;
        Suspend = suspend;
    }

    public static Result Succeed { get; } = new Result();
    public static implicit operator Result(Fail fail) => new Result(fail.Exception);
    public static implicit operator Result(Postpone postpone) => new Result(postpone);
    public static implicit operator Result(Suspend suspend) => new Result(suspend);
    public static Result<T> SucceedWithValue<T>(T value) => new(value);

    public Result<Unit> ToUnit() => Outcome switch
    {
        Outcome.Succeed => new Result<Unit>(Unit.Instance),
        Outcome.Postpone => new Result<Unit>(Postpone!),
        Outcome.Fail => new Result<Unit>(Fail!),
        Outcome.Suspend => new Result<Unit>(Suspend!),
        _ => throw new ArgumentOutOfRangeException()
    };
}

public static class Succeed
{
    public static Result WithoutValue { get; } = new Result();
    public static Result<T> WithValue<T>(T value) => new Result<T>(value);
}

public class Fail
{
    public Exception Exception { get; } 
    
    public Fail(Exception exception) => Exception = exception;

    public static Fail WithException(Exception exception) => new Fail(exception);
}

public class Postpone
{
    public DateTime DateTime { get; }

    private Postpone(DateTime dateTime) => DateTime = dateTime;

    public Result ToResult() => new(this);
    public Result<T> ToResult<T>() => new(this);
    
    public static Postpone Until(DateTime dateTime) => new(dateTime.ToUniversalTime());

    public static Postpone For(TimeSpan timeSpan) => new(DateTime.UtcNow.Add(timeSpan));

    public static Postpone For(int ms) => new(DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(ms)));
    public static void Throw(DateTime postponeUntil) => throw new PostponeInvocationException(postponeUntil);
    public static void Throw(TimeSpan postponeFor) => throw new PostponeInvocationException(DateTime.UtcNow.Add(postponeFor));
}

public class Suspend
{
    public int UntilEventSourceCount { get; }
    
    private Suspend(int untilEventSourceCount) => UntilEventSourceCount = untilEventSourceCount;

    public static Suspend Until(int eventSourceCountAtLeast) => new Suspend(eventSourceCountAtLeast);
    public static void Throw(int untilEventSourceCountAtLeast) => throw new SuspendInvocationException(untilEventSourceCountAtLeast);
}

public class Result<T>
{
    public Outcome Outcome { get; }
    public bool Succeed { get; }
    public T? SucceedWithValue { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }
    public Suspend? Suspend { get; }

    public Result(T succeedWithValue) : this(succeed: true, succeedWithValue, postpone: null, fail: null, suspend: null) {}
    public Result(Postpone postpone) : this(succeed: false, succeedWithValue: default, postpone, fail: null, suspend: null) {}
    public Result(Exception failWith) : this(succeed: false, succeedWithValue: default, postpone: null, fail: failWith, suspend: null) {}
    public Result(Suspend suspend) : this(succeed: false, succeedWithValue: default, postpone: null, fail: null, suspend) {}

    private Result(bool succeed, T? succeedWithValue, Postpone? postpone, Exception? fail, Suspend? suspend)
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
