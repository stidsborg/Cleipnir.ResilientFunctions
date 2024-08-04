using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface ITimeoutProvider
{
    Task RegisterTimeout(string timeoutId, DateTime expiresAt);
    Task RegisterTimeout(string timeoutId, TimeSpan expiresIn);
    Task CancelTimeout(string timeoutId);
    Task<IReadOnlyList<TimeoutEvent>> PendingTimeouts();
}

public class TimeoutProvider : ITimeoutProvider
{
    private readonly ITimeoutStore _timeoutStore;
    
    private Dictionary<string, TimeoutEvent>? _localTimeouts;
    private readonly object _sync = new();

    private readonly FlowId _flowId;

    public TimeoutProvider(FlowId flowId, ITimeoutStore timeoutStore)
    {
        _timeoutStore = timeoutStore;
        _flowId = flowId;
    }

    private async Task<Dictionary<string, TimeoutEvent>> GetRegisteredTimeouts()
    {
        lock (_sync)
            if (_localTimeouts is not null)
                return _localTimeouts;
        
        var timeouts = await _timeoutStore.GetTimeouts(_flowId);
        var localTimeouts = timeouts
            .ToDictionary(
                t => t.TimeoutId,
                t => new TimeoutEvent(t.TimeoutId, new DateTime(t.Expiry).ToUniversalTime())
            );
        
        lock (_sync)
            if (_localTimeouts is null)
                _localTimeouts = localTimeouts;
            else
                localTimeouts = _localTimeouts;

        return localTimeouts;
    }
    
    public async Task RegisterTimeout(string timeoutId, DateTime expiresAt)
    {
        var registeredTimeouts = await GetRegisteredTimeouts();
        lock (_sync)
            if (registeredTimeouts.ContainsKey(timeoutId))
                return;
            else
                registeredTimeouts[timeoutId] = new TimeoutEvent(timeoutId, expiresAt.ToUniversalTime());
        
        expiresAt = expiresAt.ToUniversalTime();
        await _timeoutStore.UpsertTimeout(
            new StoredTimeout(_flowId, timeoutId, expiresAt.Ticks),
            overwrite: true
        );
    }
    
    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));

    public async Task CancelTimeout(string timeoutId)
    {
        var registeredTimeouts = await GetRegisteredTimeouts();
        lock (_sync)
            if (!registeredTimeouts.Remove(timeoutId))
                return;
        
        await _timeoutStore.RemoveTimeout(_flowId, timeoutId);   
    }

    public async Task<IReadOnlyList<TimeoutEvent>> PendingTimeouts()
    {
        var registeredTimeouts = await GetRegisteredTimeouts();

        lock (_sync)
            return registeredTimeouts.Values.ToList();
    }
}