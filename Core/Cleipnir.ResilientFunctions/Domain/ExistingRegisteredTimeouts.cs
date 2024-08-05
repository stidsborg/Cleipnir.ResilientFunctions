using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingRegisteredTimeouts(FlowId flowId, ITimeoutStore timeoutStore)
{
    private Dictionary<TimeoutId, DateTime>? _timeouts;

    private async Task<Dictionary<TimeoutId, DateTime>> GetTimeouts()
    {
        if (_timeouts is not null)
            return _timeouts;

        var storedTimeouts = await timeoutStore.GetTimeouts(flowId);
        return _timeouts = storedTimeouts.ToDictionary(
            s => new TimeoutId(s.TimeoutId),
            s => new DateTime(s.Expiry, DateTimeKind.Utc)
        );
    }

    public Task<DateTime> this[TimeoutId timeoutId] => GetTimeouts().ContinueWith(t => t.Result[timeoutId]);

    public Task<IReadOnlyList<RegisteredTimeout>> All
        => GetTimeouts().ContinueWith(
            t => t.Result
                .Select(kv => new RegisteredTimeout(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<RegisteredTimeout>>()
        );

    public async Task Remove(TimeoutId timeoutId)
    {
        var timeouts = await GetTimeouts();
        
        await timeoutStore.RemoveTimeout(flowId, timeoutId.Value);
        timeouts.Remove(timeoutId);
    }

    public async Task Upsert(TimeoutId timeoutId, DateTime expiresAt)
    {
        var timeouts = await GetTimeouts();
        await timeoutStore.UpsertTimeout(
            new StoredTimeout(flowId, timeoutId.Value, expiresAt.ToUniversalTime().Ticks),
            overwrite: true
        );
        
        timeouts[timeoutId] = expiresAt;
    }
    
    public Task Upsert(TimeoutId timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
}