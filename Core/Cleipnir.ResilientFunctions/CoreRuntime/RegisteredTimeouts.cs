using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface IRegisteredTimeouts
{
    Task RegisterTimeout(TimeoutId timeoutId, DateTime expiresAt);
    Task RegisterTimeout(TimeoutId timeoutId, TimeSpan expiresIn);
    Task CancelTimeout(TimeoutId timeoutId);
    Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts();
}

public class RegisteredTimeouts(StoredId id, ITimeoutStore timeoutStore, Effect effect) : IRegisteredTimeouts
{
    private enum TimeoutStatus
    {
        Created,
        Registered,
        Cancelled
    }
    
    public string GetNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();
    
    public async Task RegisterTimeout(TimeoutId timeoutId, DateTime expiresAt)
    {
        if (await effect.Contains(timeoutId.Value, EffectType.System))
            return;
        
        expiresAt = expiresAt.ToUniversalTime();
        await timeoutStore.UpsertTimeout(
            new StoredTimeout(id, timeoutId.Value, expiresAt.Ticks),
            overwrite: true
        );

        await effect.Upsert(timeoutId.Value, TimeoutStatus.Registered, EffectType.System);
    }
    
    public Task RegisterTimeout(TimeoutId timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));

    public async Task CancelTimeout(TimeoutId timeoutId)
    {
        if (!await effect.Contains(timeoutId.Value, EffectType.System))
        {
            await timeoutStore.RemoveTimeout(id, timeoutId.Value);
            return;
        }
        
        var timeoutStatus = await effect.Get<TimeoutStatus>(timeoutId.Value, EffectType.System);
        if (timeoutStatus == TimeoutStatus.Cancelled)
            return;
        
        await timeoutStore.RemoveTimeout(id, timeoutId.Value);
        await effect.Upsert(timeoutId.Value, TimeoutStatus.Cancelled, EffectType.System);
    }

    public async Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts()
    {
        var timeouts = await timeoutStore.GetTimeouts(id);
        return timeouts
            .Select(t => new RegisteredTimeout(t.TimeoutId, new DateTime(t.Expiry).ToUniversalTime()))
            .ToList();
    }
}