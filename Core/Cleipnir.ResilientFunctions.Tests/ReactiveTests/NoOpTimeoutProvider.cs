﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Events;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class NoOpTimeoutProvider : ITimeoutProvider
{
    public static NoOpTimeoutProvider Instance { get; } = new();
    public Task RegisterTimeout(string timeoutId, DateTime expiresAt, bool overwrite = false)
        => Task.CompletedTask;

    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false)
        => Task.CompletedTask;

    public Task CancelTimeout(string timeoutId)
        => Task.CompletedTask;

    public Task<List<TimeoutEvent>> PendingTimeouts() => Task.FromResult(new List<TimeoutEvent>());
}