using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsTimeoutManager : IDisposable
{
    private readonly Lock _lock = new();
    private readonly Dictionary<StoredId, FlowEntry> _flows = new();
    private volatile bool _disposed;

    public FlowsTimeoutManager()
    {
        _ = Task.Run(PollLoop);
    }

    public void RegisterFlow(StoredId id, Action checkTimeouts)
    {
        lock (_lock)
        {
            if (!_flows.TryGetValue(id, out var entry))
            {
                entry = new FlowEntry(checkTimeouts, new Dictionary<EffectId, DateTime>());
                _flows[id] = entry;
            }
        }
    }

    public void RegisterTimeout(StoredId id, EffectId effectId, DateTime timeout)
    {
        lock (_lock)
        {
            if (!_flows.TryGetValue(id, out var entry))
                return;

            entry.Timeouts[effectId] = timeout;
        }
    }

    public void RemoveTimeout(StoredId id, EffectId effectId)
    {
        lock (_lock)
        {
            if (!_flows.TryGetValue(id, out var entry))
                return;

            entry.Timeouts.Remove(effectId);
        }
    }

    public void RemoveFlow(StoredId id)
    {
        lock (_lock)
            _flows.Remove(id);
    }

    private async Task PollLoop()
    {
        while (!_disposed)
        {
            List<Action> toSignal;
            lock (_lock)
            {
                var now = DateTime.UtcNow;
                toSignal = new List<Action>();

                foreach (var (_, entry) in _flows)
                {
                    var expiredKeys = entry.Timeouts
                        .Where(kv => kv.Value <= now)
                        .Select(kv => kv.Key)
                        .ToList();

                    if (expiredKeys.Count == 0)
                        continue;

                    foreach (var key in expiredKeys)
                        entry.Timeouts.Remove(key);

                    toSignal.Add(entry.CheckTimeouts);
                }
            }

            foreach (var checkTimeouts in toSignal)
                checkTimeouts();

            await Task.Delay(100);
        }
    }

    public void Dispose() => _disposed = true;

    private record FlowEntry(Action CheckTimeouts, Dictionary<EffectId, DateTime> Timeouts);
}
