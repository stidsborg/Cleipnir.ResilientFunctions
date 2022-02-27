using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public enum Outcome
{
    Succeeded = 1,
    Postponed = 2,
    Failed = 3
}

public class RResult<T>
{
    public FunctionId FunctionId { get; }
    public Outcome Outcome { get; }
    public T? SuccessResult { get; }
    public bool Succeeded => Outcome == Outcome.Succeeded;
    public DateTime? PostponedUntil { get; }
    public bool Postponed => Outcome == Outcome.Postponed;
    public Exception? FailedException { get; }
    public bool Failed => Outcome == Outcome.Failed;

    internal RResult(FunctionId functionId, Outcome outcome, T? successResult, DateTime? postponedUntil, Exception? failedException)
    {
        FunctionId = functionId;
        SuccessResult = successResult;
        FailedException = failedException;
        PostponedUntil = postponedUntil;
        Outcome = outcome;
    }

    public TMatched Match<TMatched>(
        Func<T, TMatched> onSuccess,
        Func<DateTime, TMatched> onPostponed,
        Func<Exception, TMatched> onFailed
    ) => Outcome switch
    {
        Outcome.Succeeded => onSuccess(SuccessResult!),
        Outcome.Postponed => onPostponed(PostponedUntil!.Value),
        Outcome.Failed => onFailed(FailedException!),
        _ => throw new ArgumentOutOfRangeException()
    };

    public void Match(
        Action<T> onSuccess,
        Action<DateTime> onPostponed,
        Action<Exception> onFailed
    )
    {
        switch (Outcome)
        {
            case Outcome.Succeeded:
                onSuccess(SuccessResult!);
                break;
            case Outcome.Postponed:
                onPostponed(PostponedUntil!.Value);
                break;
            case Outcome.Failed:
                onFailed(FailedException!);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    public T EnsureSuccess()
    {
        return Outcome switch
        {
            Outcome.Succeeded => SuccessResult!,
            Outcome.Postponed => throw new PostponedFunctionInvocationException(
                FunctionId,
                $"Function has been postponed until: '{PostponedUntil!.Value:O}'"
            ),
            Outcome.Failed => throw FailedException!,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public override string ToString() => Outcome switch
    {
        Outcome.Succeeded => $"{FunctionId} succeeded with: '{SuccessResult?.ToString() ?? "NULL"}'",
        Outcome.Postponed => $"{FunctionId} postponed until: '{PostponedUntil!.Value:O}'",
        Outcome.Failed => $"{FunctionId} failed with: {FailedException}",
        _ => throw new ArgumentOutOfRangeException()
    };
}

public class RResult
{
    public FunctionId FunctionId { get; }
    public Outcome Outcome { get; }
    public bool Succeeded => Outcome == Outcome.Succeeded;
    public DateTime? PostponedUntil { get; }
    public bool Postponed => Outcome == Outcome.Postponed;
    public Exception? FailedException { get; }
    public bool Failed => Outcome == Outcome.Failed;

    internal RResult(FunctionId functionId, Outcome outcome, DateTime? postponedUntil, Exception? failedException)
    {
        FunctionId = functionId;
        FailedException = failedException;
        PostponedUntil = postponedUntil;
        Outcome = outcome;
    }

    public TMatched Match<TMatched>(
        Func<TMatched> onSuccess,
        Func<DateTime, TMatched> onPostponed,
        Func<Exception, TMatched> onFailed
    ) => Outcome switch
    {
        Outcome.Succeeded => onSuccess(),
        Outcome.Postponed => onPostponed(PostponedUntil!.Value),
        Outcome.Failed => onFailed(FailedException!),
        _ => throw new ArgumentOutOfRangeException()
    };

    public void Match(
        Action onSuccess,
        Action<DateTime> onPostponed,
        Action<Exception> onFailed
    )
    {
        switch (Outcome)
        {
            case Outcome.Succeeded:
                onSuccess();
                break;
            case Outcome.Postponed:
                onPostponed(PostponedUntil!.Value);
                break;
            case Outcome.Failed:
                onFailed(FailedException!);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public void EnsureSuccess()
    {
        switch (Outcome)
        {
            case Outcome.Succeeded:
                return;
            case Outcome.Postponed:
                throw new InvalidOperationException(
                    $"Function has been postponed until: '{PostponedUntil!.Value:O}'"
                );
            case Outcome.Failed:
                throw new InvalidOperationException(
                    "Function invocation failed", FailedException!
                );
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    public override string ToString()
        => Outcome switch
        {
            Outcome.Succeeded => $"{FunctionId} succeeded",
            Outcome.Postponed => $"{FunctionId} postponed until: '{PostponedUntil!.Value:O}'",
            Outcome.Failed => $"{FunctionId} failed with: {FailedException}",
            _ => throw new ArgumentOutOfRangeException()
        };
}