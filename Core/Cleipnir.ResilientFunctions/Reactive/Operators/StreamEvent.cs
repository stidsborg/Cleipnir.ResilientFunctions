using System;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public readonly struct StreamEvent<T>
{
    public bool SignalCompletion { get; }
    public Exception? Error { get; }
    public T Next { get; }

    public StreamEventStatus Status { get; }

    public StreamEvent(bool signalCompletion, Exception? error, T next)
    {
        SignalCompletion = signalCompletion;
        Error = error;
        Next = next;
        Status = signalCompletion
            ? StreamEventStatus.SignalCompletion
            : error != null
                ? StreamEventStatus.SignalError
                : StreamEventStatus.SignalNext;
    }

    public static StreamEvent<T> CreateFromCompletion() 
        => new(signalCompletion: true, error: null, next: default!);
    public static StreamEvent<T> CreateFromException(Exception exception) 
        => new(signalCompletion: false, error: exception, next: default!);
    public static StreamEvent<T> CreateFromNext(T next) 
        => new(signalCompletion: false, error: null, next: next);
}

public enum StreamEventStatus
{
    SignalNext,
    SignalCompletion,
    SignalError,
}