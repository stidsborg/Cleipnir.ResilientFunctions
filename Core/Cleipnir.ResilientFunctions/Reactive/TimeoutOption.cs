using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using EventSource = Cleipnir.ResilientFunctions.Messaging.EventSource;

namespace Cleipnir.ResilientFunctions.Reactive;

public record TimeoutOption<T>(bool TimedOut, T? Value)
{
    public T EnsureNoTimeout()
        => TimedOut
            ? throw new TimeoutException("Event was not emitted within timeout")
            : Value!;
}

public record TimeoutOption(bool TimedOut);

public static class TimeoutOptionExtensions
{
    public static async Task<T> EnsureNoTimeout<T>(this Task<TimeoutOption<T>> timeoutOption)
        => (await timeoutOption).EnsureNoTimeout();

    public static async Task RegisterTimeoutEvent(this EventSource eventSource, string timeoutId, DateTime expiresAt)
    {
        if (!DoesEventSourceAlreadyContainTimeoutEvent(eventSource, timeoutId))
            await eventSource.TimeoutProvider.RegisterTimeout(timeoutId, expiresAt);
    }

    public static async Task RegisterTimeoutEvent(this EventSource eventSource, string timeoutId, TimeSpan expiresIn)
    {
        if (!DoesEventSourceAlreadyContainTimeoutEvent(eventSource, timeoutId))
            await eventSource.TimeoutProvider.RegisterTimeout(timeoutId, expiresIn);
    }
    
    public static async Task CancelTimeoutEvent(this EventSource eventSource, string timeoutId)
    {
        if (!DoesEventSourceAlreadyContainTimeoutEvent(eventSource, timeoutId))
            await eventSource.TimeoutProvider.CancelTimeout(timeoutId);
    }

    private static bool DoesEventSourceAlreadyContainTimeoutEvent(EventSource eventSource, string timeoutId)
        => eventSource
            .PullExisting()
            .OfType<TimeoutEvent>()
            .Any(t => t.TimeoutId == timeoutId);
        
}