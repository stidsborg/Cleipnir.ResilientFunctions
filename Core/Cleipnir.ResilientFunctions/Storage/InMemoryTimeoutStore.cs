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
    public Task Truncate()
    {
        lock (_sync)
            _timeouts.Clear();

        return Task.CompletedTask;
    }

    public Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var ((flowType, flowInstance), timeoutId, expiry) = storedTimeout;
        var key = new Key(flowType.Value, flowInstance.Value, timeoutId);
        lock (_sync)
            if (!_timeouts.ContainsKey(key) || overwrite)
                _timeouts[key] = expiry;

        return Task.CompletedTask;
    }

    public Task RemoveTimeout(FlowId flowId, string timeoutId)
    {
        var key = new Key(flowId.Type.Value, flowId.Instance.Value, timeoutId);
        lock (_sync)
            _timeouts.Remove(key);

        return Task.CompletedTask;
    }

    public Task Remove(FlowId flowId)
    {
        lock (_sync)
            foreach (var key in _timeouts.Keys.ToList())
                if (flowId.Type == key.FlowType && flowId.Instance == key.FlowInstance)
                    _timeouts.Remove(key);

        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore)
    {
        lock (_sync)
            return _timeouts
                .Where(kv => kv.Value <= expiresBefore)
                .Select(kv =>
                {
                    var functionId = new FlowId(kv.Key.FlowType, kv.Key.FlowInstance);
                    return new StoredTimeout(functionId, kv.Key.TimeoutId, kv.Value);
                })
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    public Task<IEnumerable<StoredTimeout>> GetTimeouts(FlowId flowId)
    {
        lock (_sync)
            return _timeouts.Where(kv =>
                    kv.Key.FlowType == flowId.Type &&
                    kv.Key.FlowInstance == flowId.Instance
                )
                .Select(kv => new StoredTimeout(flowId, kv.Key.TimeoutId, Expiry: kv.Value))
                .ToList()
                .AsEnumerable()
                .ToTask();
    }

    private record Key(string FlowType, string FlowInstance, string TimeoutId);
}