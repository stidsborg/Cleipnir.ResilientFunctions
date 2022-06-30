using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class Result
{
    public Status Status { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }

    public Result() : this(succeed: true, postpone: null, fail: null) {}
    public Result(Postpone postpone) : this(succeed: false, postpone, fail: null) {}
    public Result(Exception exception) : this(succeed: false, postpone: null, fail: exception) {}

    private Result(bool succeed, Postpone? postpone, Exception? fail)
    {
        if (succeed)
            Status = Status.Succeeded;
        else if (postpone != null)
            Status = Status.Postponed;
        else
            Status = Status.Failed;
        
        Postpone = postpone;
        Fail = fail;
        Postpone = postpone;
    }

    public static Result Succeed { get; } = new Result();
    public static implicit operator Result(Fail fail) => new Result(fail.Exception);
    public static implicit operator Result(Postpone postpone) => new Result(postpone);
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
}

public class Result<T>
{
    public Status Status { get; }
    public bool Succeed => Status == Status.Succeeded;
    public T? SucceedWithValue { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }

    public Result(T succeedWithValue) : this(succeed: true, succeedWithValue, postpone: null, fail: null) {}
    public Result(Postpone postpone) : this(
        succeed: false, 
        succeedWithValue: default, 
        postpone, 
        fail: null
    ) {}
    
    public Result(Exception failWith) : this(succeed: false, succeedWithValue: default, postpone: null, fail: failWith) {}
    
    private Result(bool succeed, T? succeedWithValue, Postpone? postpone, Exception? fail)
    {
        if (succeed)
            Status = Status.Succeeded;
        else if (postpone != null)
            Status = Status.Postponed;
        else
            Status = Status.Failed;

        SucceedWithValue = succeedWithValue;
        Postpone = postpone;
        Fail = fail;
    }

    public static implicit operator Result<T>(T succeedWithValue) => new Result<T>(succeedWithValue);
    public static implicit operator Result<T>(Fail fail) => new Result<T>(fail.Exception);
    public static implicit operator Result<T>(Postpone postpone) => new Result<T>(postpone);
}