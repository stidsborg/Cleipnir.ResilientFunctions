using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace Cleipnir.ResilientFunctions.Reactive;

public static class TimeoutExtensions
{
    public static async Task RegisterTimeoutEvent(this Messages messages, string timeoutId, DateTime expiresAt)
    {
        if (!DoesMessagesAlreadyContainTimeoutEvent(messages, timeoutId))
            await messages.TimeoutProvider.RegisterTimeout(timeoutId, expiresAt);
    }

    public static async Task RegisterTimeoutEvent(this Messages messages, string timeoutId, TimeSpan expiresIn)
    {
        if (!DoesMessagesAlreadyContainTimeoutEvent(messages, timeoutId))
            await messages.TimeoutProvider.RegisterTimeout(timeoutId, expiresIn);
    }
    
    public static async Task CancelTimeoutEvent(this Messages messages, string timeoutId)
    {
        if (!DoesMessagesAlreadyContainTimeoutEvent(messages, timeoutId))
            await messages.TimeoutProvider.CancelTimeout(timeoutId);
    }

    private static bool DoesMessagesAlreadyContainTimeoutEvent(Messages messages, string timeoutId)
        => messages
            .Existing()
            .OfType<TimeoutEvent>()
            .Any(t => t.TimeoutId == timeoutId);
}