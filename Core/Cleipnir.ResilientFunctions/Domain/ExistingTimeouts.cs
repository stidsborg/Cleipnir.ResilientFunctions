using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingTimeouts
{
    private readonly FunctionId _functionId;
    private readonly ITimeoutStore _timeoutStore;
    private readonly Dictionary<string, DateTime> _timeouts;
    
    public ExistingTimeouts(FunctionId functionId, ITimeoutStore timeoutStore, IEnumerable<StoredTimeout> storedTimeouts)
    {
        _functionId = functionId;
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
        await _timeoutStore.RemoveTimeout(_functionId, timeoutId);
        
        _timeouts.Remove(timeoutId);
    }

    public async Task Upsert(string timeoutId, DateTime expiresAt)
    {
        await _timeoutStore.UpsertTimeout(
            new StoredTimeout(_functionId, timeoutId, expiresAt.ToUniversalTime().Ticks),
            overwrite: true
        );
        
        _timeouts[timeoutId] = expiresAt;
    }
    
    public Task Upsert(string timeoutId, TimeSpan expiresIn) 
        => Upsert(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
}