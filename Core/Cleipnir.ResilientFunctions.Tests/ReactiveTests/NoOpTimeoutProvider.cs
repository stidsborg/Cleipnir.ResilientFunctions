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

    public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, DateTime expiresAt, bool publishMessage)
        => Tuple.Create(TimeoutStatus.Registered, expiresAt).ToTask();

    public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn, bool publishMessage)
        => Tuple.Create(TimeoutStatus.Registered, DateTime.UtcNow.Add(expiresIn)).ToTask();
    
    public Task CancelTimeout(EffectId timeoutId)
        => Task.CompletedTask;

    public Task CompleteTimeout(EffectId timeoutId) 
        => Task.CompletedTask;

    public Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts() => new List<RegisteredTimeout>()
        .CastTo<IReadOnlyList<RegisteredTimeout>>()
        .ToTask();
    
    public void Dispose() { }
}