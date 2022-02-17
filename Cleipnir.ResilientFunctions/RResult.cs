using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public class RResult<T>
{
    public ResultType ResultType { get; }
    public T? SuccessResult { get; }
    public bool Succeeded => ResultType == ResultType.Succeeded;
    public DateTime? PostponedUntil { get; }
    public bool Postponed => ResultType == ResultType.Postponed;
    public Exception? FailedException { get; }
    public bool Failed => ResultType == ResultType.Failed;

    internal RResult(ResultType resultType, T? successResult, DateTime? postponedUntil, Exception? failedException)
    {
        SuccessResult = successResult;
        FailedException = failedException;
        PostponedUntil = postponedUntil;
        ResultType = resultType;
    }

    public TMatched Match<TMatched>(
        Func<T, TMatched> onSuccess,
        Func<DateTime, TMatched> onPostponed,
        Func<Exception, TMatched> onFailed
    ) => ResultType switch
    {
        ResultType.Succeeded => onSuccess(SuccessResult!),
        ResultType.Postponed => onPostponed(PostponedUntil!.Value),
        ResultType.Failed => onFailed(FailedException!),
        _ => throw new ArgumentOutOfRangeException()
    };

    public void Match(
        Action<T> onSuccess,
        Action<DateTime> onPostponed,
        Action<Exception> onFailed
    )
    {
        switch (ResultType)
        {
            case ResultType.Succeeded:
                onSuccess(SuccessResult!);
                break;
            case ResultType.Postponed:
                onPostponed(PostponedUntil!.Value);
                break;
            case ResultType.Failed:
                onFailed(FailedException!);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    public T EnsureSuccess()
    {
        return ResultType switch
        {
            ResultType.Succeeded => SuccessResult!,
            ResultType.Postponed => throw new FunctionInvocationUnhandledException($"Function has been postponed until: '{PostponedUntil!.Value:O}'"),
            ResultType.Failed => throw new FunctionInvocationUnhandledException("Function invocation failed", FailedException!),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public static implicit operator RResult<T>(T result) => new(
        ResultType.Succeeded,
        result, 
        postponedUntil: null, 
        failedException: null
    );
    
    public static implicit operator RResult<T>(Fail fail) => new(
        ResultType.Failed, 
        default,
        postponedUntil: null,
        fail.Exception
    );
    
    public static implicit operator RResult<T>(Postpone postpone) => new(
        ResultType.Postponed, 
        successResult: default,
        postpone.PostponeUntil, 
        failedException: null
    );

    public override string ToString() => ResultType switch
    {
        ResultType.Succeeded => $"Succeeded with: '{SuccessResult?.ToString() ?? "NULL"}'",
        ResultType.Postponed => $"Postponed until: '{PostponedUntil!.Value:O}'",
        ResultType.Failed => $"Failed with: {FailedException}",
        _ => throw new ArgumentOutOfRangeException()
    };
}

public enum ResultType
{
    Succeeded = 1,
    Postponed = 2,
    Failed = 3
}

public sealed class Succeed
{
    public static RResult<T> WithResult<T>(T result) =>
        new RResult<T>(
            ResultType.Succeeded,
            result,
            postponedUntil: null,
            failedException: null
        );

    public static RResult WithoutResult() => 
        new RResult(
            ResultType.Succeeded, 
            postponedUntil: null, 
            failedException: null
        );
}

public sealed class Fail
{
    public Exception Exception { get; }
    
    public Fail(Exception exception) => Exception = exception;
    public static Fail WithException(Exception exception) => new Fail(exception);
}

public sealed class Postpone
{
    public DateTime PostponeUntil { get; }
    
    public Postpone(DateTime postponeUntilUntil) => PostponeUntil = postponeUntilUntil;
    
    public static Postpone For(TimeSpan delay) => new(DateTime.UtcNow.Add(delay));
    public static Postpone For(int delayMs) => For(TimeSpan.FromMilliseconds(delayMs));
    public static Postpone Until(DateTime dateTime) => new(dateTime.ToUniversalTime());
}

public class RResult
{
    public static RResult Success { get; } = new(ResultType.Succeeded, postponedUntil: null, failedException: null);
    public ResultType ResultType { get; }
    public bool Succeeded => ResultType == ResultType.Succeeded;
    public DateTime? PostponedUntil { get; }
    public bool Postponed => ResultType == ResultType.Postponed;
    public Exception? FailedException { get; }
    public bool Failed => ResultType == ResultType.Failed;

    internal RResult(ResultType resultType, DateTime? postponedUntil, Exception? failedException)
    {
        FailedException = failedException;
        PostponedUntil = postponedUntil;
        ResultType = resultType;
    }

    public TMatched Match<TMatched>(
        Func<TMatched> onSuccess,
        Func<DateTime, TMatched> onPostponed,
        Func<Exception, TMatched> onFailed
    ) => ResultType switch
    {
        ResultType.Succeeded => onSuccess(),
        ResultType.Postponed => onPostponed(PostponedUntil!.Value),
        ResultType.Failed => onFailed(FailedException!),
        _ => throw new ArgumentOutOfRangeException()
    };

    public void Match(
        Action onSuccess,
        Action<DateTime> onPostponed,
        Action<Exception> onFailed
    )
    {
        switch (ResultType)
        {
            case ResultType.Succeeded:
                onSuccess();
                break;
            case ResultType.Postponed:
                onPostponed(PostponedUntil!.Value);
                break;
            case ResultType.Failed:
                onFailed(FailedException!);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public void EnsureSuccess()
    {
        switch (ResultType)
        {
            case ResultType.Succeeded:
                return;
            case ResultType.Postponed:
                throw new FunctionInvocationUnhandledException($"Function has been postponed until: '{PostponedUntil!.Value:O}'");
            case ResultType.Failed:
                throw new FunctionInvocationUnhandledException("Function invocation failed", FailedException!);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    public static implicit operator RResult(Fail fail) => new(
        ResultType.Failed,
        postponedUntil: null,
        fail.Exception
    );
    
    public static implicit operator RResult(Postpone postpone) => new(
        ResultType.Postponed,
        postpone.PostponeUntil, 
        failedException: null
    );

    public override string ToString()
        => ResultType switch
        {
            ResultType.Succeeded => "Succeeded",
            ResultType.Postponed => $"Postponed until: '{PostponedUntil!.Value:O}'",
            ResultType.Failed => $"Failed with: {FailedException}",
            _ => throw new ArgumentOutOfRangeException()
        };
}

public static class RResultExtensions
{
    public static RResult<T> ToFailedRResult<T>(this Exception exception)
        => new(ResultType.Failed, successResult: default, postponedUntil: null, exception);
    
    public static RResult ToFailedRResult(this Exception exception)
        => new(ResultType.Failed, postponedUntil: null, exception);
    
    public static RResult ToPostponedRResult(this TimeSpan @for)
        => new(ResultType.Postponed, postponedUntil: DateTime.UtcNow.Add(@for), failedException: null);
    
    public static RResult<T> ToPostponedRResult<T>(this TimeSpan @for)
        => new(
            ResultType.Postponed, 
            successResult: default, 
            postponedUntil: DateTime.UtcNow.Add(@for), 
            failedException: null
        );
    
    public static RResult ToPostponedRResult(this DateTime until)
        => new(ResultType.Postponed, postponedUntil: until.ToUniversalTime(), failedException: null);
    
    public static RResult<T> ToPostponedRResult<T>(this DateTime until)
        => new(
            ResultType.Postponed, 
            successResult: default, 
            postponedUntil: until.ToUniversalTime(), 
            failedException: null
        );
    
    public static RResult<T> ToSucceededRResult<T>(this T result)
        => new(ResultType.Succeeded, successResult: result, postponedUntil: null, failedException: null);
}