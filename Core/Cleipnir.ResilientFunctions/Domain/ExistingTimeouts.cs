using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingTimeouts
{
    private readonly FlowId _flowId;
    private readonly ITimeoutStore _timeoutStore;
    private readonly Dictionary<string, DateTime> _timeouts;
    
    public ExistingTimeouts(FlowId flowId, ITimeoutStore timeoutStore, IEnumerable<StoredTimeout> storedTimeouts)
    {
        _flowId = flowId;
        _timeoutStore = timeoutStore;
        _timeouts = storedTimeouts.ToDictionary(s => s.TimeoutId, s => new DateTime(s.Expiry, DateTimeKind.Utc));
    }
    
    public DateTime this[string timeoutId] => _timeouts[timeoutId];
    
    public IReadOnlyList<TimeoutEvent> All 
        => _timeouts
            .Select(kv => new TimeoutEvent(kv.Key, kv.Value))
            .ToList();

    public async Task Remove(string timeoutId)
    {
        await _timeoutStore.RemoveTimeout(_flowId, timeoutId);
        
        _timeouts.Remove(timeoutId);
    }

    public async Task Upsert(string timeoutId, DateTime expiresAt)
    {
        await _timeoutStore.UpsertTimeout(
            new StoredTimeout(_flowId, timeoutId, expiresAt.ToUniversalTime().Ticks),
            overwrite: true
        );
        
        _timeouts[timeoutId] = expiresAt;
    }
    
    public Task Upsert(string timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
}