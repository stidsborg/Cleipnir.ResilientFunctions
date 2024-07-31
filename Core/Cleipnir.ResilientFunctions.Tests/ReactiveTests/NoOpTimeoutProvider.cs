using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class NoOpTimeoutProvider : ITimeoutProvider
{
    public static NoOpTimeoutProvider Instance { get; } = new();
    public Task RegisterTimeout(string timeoutId, DateTime expiresAt)
        => Task.CompletedTask;

    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
        => Task.CompletedTask;

    public Task CancelTimeout(string timeoutId)
        => Task.CompletedTask;

    public Task<IReadOnlyList<TimeoutEvent>> PendingTimeouts() => new List<TimeoutEvent>()
        .CastTo<IReadOnlyList<TimeoutEvent>>()
        .ToTask();
}