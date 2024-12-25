using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

public class NoOpRegisteredTimeouts : IRegisteredTimeouts
{
    public static NoOpRegisteredTimeouts Instance { get; } = new();
    public Task RegisterTimeout(EffectId timeoutId, DateTime expiresAt)
        => Task.CompletedTask;

    public Task RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn)
        => Task.CompletedTask;

    public Task CancelTimeout(EffectId timeoutId)
        => Task.CompletedTask;

    public Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts() => new List<RegisteredTimeout>()
        .CastTo<IReadOnlyList<RegisteredTimeout>>()
        .ToTask();
}