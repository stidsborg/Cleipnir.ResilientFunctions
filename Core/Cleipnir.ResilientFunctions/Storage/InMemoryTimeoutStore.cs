using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryTimeoutStore : ITimeoutStore
{
    private readonly Dictionary<Key, long> _timeouts = new();
    private readonly object _sync = new();
    
    public Task Initialize() => Task.CompletedTask;

    public Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var ((functionTypeId, functionInstanceId), timeoutId, expiry) = storedTimeout;
        var key = new Key(functionTypeId.Value, functionInstanceId.Value, timeoutId);
        lock (_sync)
            if (!_timeouts.ContainsKey(key) || overwrite)
                _timeouts[key] = expiry;

        return Task.CompletedTask;
    }

    public Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        var key = new Key(functionId.TypeId.Value, functionId.InstanceId.Value, timeoutId);
        lock (_sync)
            _timeouts.Remove(key);

        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        lock (_sync)
            return _timeouts
                .Where(kv => kv.Key.FunctionTypeId == functionTypeId)
                .Where(kv => kv.Value <= expiresBefore)
                .Select(kv =>
                {
                    var functionId = new FunctionId(kv.Key.FunctionTypeId, kv.Key.FunctionInstanceId);
                    return new StoredTimeout(functionId, kv.Key.TimeoutId, kv.Value);
                })
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredTimeout>> GetTimeouts(FunctionId functionId)
    {
        lock (_sync)
            return _timeouts.Where(kv =>
                    kv.Key.FunctionTypeId == functionId.TypeId &&
                    kv.Key.FunctionInstanceId == functionId.InstanceId
                )
                .Select(kv => new StoredTimeout(functionId, kv.Key.TimeoutId, Expiry: kv.Value))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    private record Key(string FunctionTypeId, string FunctionInstanceId, string TimeoutId);
}