using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingRegisteredTimeouts(ExistingEffects effects, UtcNow utcNow)
{
    private Dictionary<EffectId, Tuple<DateTime, TimeoutStatus>>? _timeouts;

    private async Task<Dictionary<EffectId, Tuple<DateTime, TimeoutStatus>>> GetTimeouts()
    {
        if (_timeouts is not null)
            return _timeouts;

        var effectIds = (await effects.AllIds).ToList();
        var timeouts = new Dictionary<EffectId, Tuple<DateTime, TimeoutStatus>>();
        foreach (var effectId in effectIds)
        {
            try
            {
                var value = await effects.GetValue<string>(effectId);
                if (value == null) continue;
                var values = value.Split("_");
                if (values.Length != 2) continue;
                var status = values[0].ToInt().ToEnum<TimeoutStatus>();
                var expiry = values[1].ToLong().ToUtc();
                timeouts[effectId] = Tuple.Create(expiry, status);
            }
            catch
            {
                // Skip non-timeout effects
            }
        }

        return _timeouts = timeouts;
    }

    public Task<DateTime> this[TimeoutId timeoutId] => this[new EffectId(timeoutId.Value, Context: "")];
    public Task<DateTime> this[EffectId timeoutId] => GetTimeouts().ContinueWith(t => t.Result[timeoutId].Item1);

    public Task<IReadOnlyList<RegisteredTimeout>> All
        => GetTimeouts().ContinueWith(
            t => t.Result
                .Select(kv => new RegisteredTimeout(kv.Key, kv.Value.Item1, kv.Value.Item2))
                .ToList()
                .CastTo<IReadOnlyList<RegisteredTimeout>>()
        );
    
    public Task Upsert(TimeoutId timeoutId, DateTime expiresAt)
        => Upsert(new EffectId(timeoutId.Value, Context: ""), expiresAt);
    public async Task Upsert(EffectId timeoutId, DateTime expiresAt)
    {
        expiresAt = expiresAt.ToUniversalTime();
        var timeouts = await GetTimeouts();
        await effects.SetValue(timeoutId, $"{(int)TimeoutStatus.Registered}_{expiresAt.Ticks}");
        timeouts[timeoutId] = new Tuple<DateTime, TimeoutStatus>(expiresAt, TimeoutStatus.Registered);
    }
    public Task Upsert(TimeoutId timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: utcNow().Add(expiresIn));
    public Task Upsert(EffectId timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: utcNow().Add(expiresIn));
}