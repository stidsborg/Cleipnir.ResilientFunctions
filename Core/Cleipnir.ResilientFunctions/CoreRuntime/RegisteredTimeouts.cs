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

public class RegisteredTimeouts(FlowId flowId, ITimeoutStore timeoutStore) : IRegisteredTimeouts
{
    private readonly ImplicitIds _implicitIds = new();
    private Dictionary<TimeoutId, RegisteredTimeout>? _localTimeouts;
    private readonly object _sync = new();

    private async Task<Dictionary<TimeoutId, RegisteredTimeout>> GetRegisteredTimeouts()
    {
        lock (_sync)
            if (_localTimeouts is not null)
                return _localTimeouts;
        
        var timeouts = await timeoutStore.GetTimeouts(flowId);
        var localTimeouts = timeouts
            .ToDictionary(
                t => new TimeoutId(t.TimeoutId),
                t => new RegisteredTimeout(t.TimeoutId, new DateTime(t.Expiry).ToUniversalTime())
            );
        
        lock (_sync)
            if (_localTimeouts is null)
                _localTimeouts = localTimeouts;
            else
                localTimeouts = _localTimeouts;

        return localTimeouts;
    }

    public string GetNextImplicitId() => _implicitIds.Next();
    
    public async Task RegisterTimeout(TimeoutId timeoutId, DateTime expiresAt)
    {
        var registeredTimeouts = await GetRegisteredTimeouts();
        lock (_sync)
            if (registeredTimeouts.ContainsKey(timeoutId.Value))
                return;
        
        expiresAt = expiresAt.ToUniversalTime();
        await timeoutStore.UpsertTimeout(
            new StoredTimeout(flowId, timeoutId.Value, expiresAt.Ticks),
            overwrite: true
        );
        
        lock (_sync)
            registeredTimeouts[timeoutId] = new RegisteredTimeout(timeoutId, expiresAt.ToUniversalTime());
    }
    
    public Task RegisterTimeout(TimeoutId timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));

    public async Task CancelTimeout(TimeoutId timeoutId)
    {
        var registeredTimeouts = await GetRegisteredTimeouts();
        lock (_sync)
            if (!registeredTimeouts.Remove(timeoutId))
                return;
        
        await timeoutStore.RemoveTimeout(flowId, timeoutId.Value);   
    }

    public async Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts()
    {
        var registeredTimeouts = await GetRegisteredTimeouts();

        lock (_sync)
            return registeredTimeouts.Values.ToList();
    }
}