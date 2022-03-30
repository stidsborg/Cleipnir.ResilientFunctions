using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class Return
{
    public Intent Intent { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }

    public Return() : this(succeed: true, postpone: null, fail: null) {}
    public Return(Postpone postpone) : this(succeed: false, postpone, fail: null) {}
    public Return(Exception exception) : this(succeed: false, postpone: null, fail: exception) {}

    private Return(bool succeed, Postpone? postpone, Exception? fail)
    {
        if (succeed)
            Intent = Intent.Succeed;
        else if (postpone != null)
            Intent = Intent.Postpone;
        else
            Intent = Intent.Fail;
        
        Postpone = postpone;
        Fail = fail;
        Postpone = postpone;
    }

    public static Return Succeed { get; } = new Return();
    public static implicit operator Return(Fail fail) => new Return(fail.Exception);
    public static implicit operator Return(Postpone postpone) => new Return(postpone);
}

public static class Succeed
{
    public static Return WithoutValue { get; } = new Return();
    public static Return<T> WithValue<T>(T value) => new Return<T>(value);
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
    public bool InProcessWait { get; }
    
    private Postpone(DateTime dateTime, bool inProcessWait)
    {
        DateTime = dateTime;
        InProcessWait = inProcessWait;
    }

    public static Postpone Until(DateTime dateTime, bool inProcessWait) 
        => new Postpone(dateTime.ToUniversalTime(), inProcessWait);
    public static Postpone For(TimeSpan timeSpan, bool inProcessWait) 
        => new Postpone(DateTime.UtcNow.Add(timeSpan), inProcessWait);
    public static Postpone For(int ms, bool inProcessWait) 
        => new Postpone(DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(ms)), inProcessWait);
}

public class Return<T>
{
    public Intent Intent { get; }
    public bool Succeed { get; }
    public T? SucceedWithValue { get; }
    public Postpone? Postpone { get; }
    public Exception? Fail { get; }

    public Return(T succeedWithValue) : this(succeed: true, succeedWithValue, postpone: null, fail: null) {}
    public Return(Postpone postpone) : this(
        succeed: false, 
        succeedWithValue: default, 
        postpone, 
        fail: null
    ) {}
    
    public Return(Exception failWith) : this(succeed: false, succeedWithValue: default, postpone: null, fail: failWith) {}
    
    private Return(bool succeed, T? succeedWithValue, Postpone? postpone, Exception? fail)
    {
        if (succeed)
            Intent = Intent.Succeed;
        else if (postpone != null)
            Intent = Intent.Postpone;
        else
            Intent = Intent.Fail;
        
        Succeed = succeed;
        SucceedWithValue = succeedWithValue;
        Postpone = postpone;
        Fail = fail;
    }

    public static implicit operator Return<T>(T succeedWithValue) => new Return<T>(succeedWithValue);
    public static implicit operator Return<T>(Fail fail) => new Return<T>(fail.Exception);
    public static implicit operator Return<T>(Postpone postpone) => new Return<T>(postpone);
}

public enum Intent
{
    Succeed = 1,
    Postpone = 2,
    Fail = 3
}
