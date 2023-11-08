using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace Cleipnir.ResilientFunctions.Reactive;

public static class TimeoutExtensions
{
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
            .Existing()
            .OfType<TimeoutEvent>()
            .Any(t => t.TimeoutId == timeoutId);
}