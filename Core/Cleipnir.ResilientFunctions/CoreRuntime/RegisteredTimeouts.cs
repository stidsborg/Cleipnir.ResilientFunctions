using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface IRegisteredTimeouts
{
    Task RegisterTimeout(EffectId timeoutId, DateTime expiresAt);
    Task RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn);
    Task CancelTimeout(EffectId timeoutId);
    Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts();
}

public enum TimeoutStatus
{
    Created,
    Registered,
    Cancelled
}

public class RegisteredTimeouts(StoredId storedId, ITimeoutStore timeoutStore, Effect effect) : IRegisteredTimeouts
{

    
    public string GetNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();
    
    public async Task RegisterTimeout(EffectId timeoutId, DateTime expiresAt)
    {
        if (await effect.Contains(timeoutId))
            return;
        
        expiresAt = expiresAt.ToUniversalTime();
        await timeoutStore.UpsertTimeout(
            new StoredTimeout(storedId, timeoutId, expiresAt.Ticks),
            overwrite: true
        );

        await effect.Upsert(timeoutId, TimeoutStatus.Registered);
    }
    
    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(EffectId.CreateWithCurrentContext(timeoutId, EffectType.Timeout), expiresAt: DateTime.UtcNow.Add(expiresIn));
    public Task RegisterTimeout(string timeoutId, DateTime expiresAt)
        => RegisterTimeout(EffectId.CreateWithCurrentContext(timeoutId, EffectType.Timeout), expiresAt);
    public Task RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));

    public async Task CancelTimeout(EffectId timeoutId)
    {
        if (!await effect.Contains(timeoutId))
        {
            await timeoutStore.RemoveTimeout(storedId, timeoutId);
            return;
        }
        
        var timeoutStatus = await effect.Get<TimeoutStatus>(timeoutId);
        if (timeoutStatus == TimeoutStatus.Cancelled)
            return;
        
        await timeoutStore.RemoveTimeout(storedId, timeoutId);
        await effect.Upsert(timeoutId, TimeoutStatus.Cancelled);
    }

    public async Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts()
    {
        var timeouts = await timeoutStore.GetTimeouts(storedId);
        return timeouts
            .Select(t => new RegisteredTimeout(t.TimeoutId, new DateTime(t.Expiry).ToUniversalTime()))
            .ToList();
    }
}