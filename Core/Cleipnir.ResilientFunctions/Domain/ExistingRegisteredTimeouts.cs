using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingRegisteredTimeouts(StoredId storedId, ITimeoutStore timeoutStore)
{
    private Dictionary<EffectId, DateTime>? _timeouts;

    private async Task<Dictionary<EffectId, DateTime>> GetTimeouts()
    {
        if (_timeouts is not null)
            return _timeouts;

        var storedTimeouts = await timeoutStore.GetTimeouts(storedId);
        return _timeouts = storedTimeouts.ToDictionary(
            s => s.TimeoutId,
            s => new DateTime(s.Expiry, DateTimeKind.Utc)
        );
    }

    public Task<DateTime> this[TimeoutId timeoutId] => this[new EffectId(timeoutId.Value, EffectType.System, Context: "")];
    public Task<DateTime> this[EffectId timeoutId] => GetTimeouts().ContinueWith(t => t.Result[timeoutId]);

    public Task<IReadOnlyList<RegisteredTimeout>> All
        => GetTimeouts().ContinueWith(
            t => t.Result
                .Select(kv => new RegisteredTimeout(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<RegisteredTimeout>>()
        );

    public Task Remove(TimeoutId timeoutId) => Remove(new EffectId(timeoutId.Value, EffectType.System, Context: ""));
    public async Task Remove(EffectId timeoutId)
    {
        var timeouts = await GetTimeouts();
        await timeoutStore.RemoveTimeout(storedId, timeoutId);
        timeouts.Remove(timeoutId);
    }
    
    public Task Upsert(TimeoutId timeoutId, DateTime expiresAt)
        => Upsert(new EffectId(timeoutId.Value, EffectType.System, Context: ""), expiresAt);
    public async Task Upsert(EffectId timeoutId, DateTime expiresAt)
    {
        var timeouts = await GetTimeouts();
        await timeoutStore.UpsertTimeout(
            new StoredTimeout(storedId, timeoutId, expiresAt.ToUniversalTime().Ticks),
            overwrite: true
        );
        
        timeouts[timeoutId] = expiresAt;
    }
    public Task Upsert(TimeoutId timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
    public Task Upsert(EffectId timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
}