using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public record FlowStatus(StoredId Id, Action Suspend, QueueManager QueueManager, int Threads, int SuspendedThreads, FlowTimeouts Timeouts);

public class FlowsManager : IDisposable
{
    private readonly Dictionary<StoredId, FlowStatus> _dict = new();
    private readonly Lock _lock = new();
    private readonly UtcNow _utcNow;
    private volatile bool _disposed;

    public FlowsManager(UtcNow utcNow)
    {
        _utcNow = utcNow;
        _ = Task.Run(TimeoutCheckLoop);
    }

    private async Task TimeoutCheckLoop()
    {
        while (!_disposed)
        {
            var expiredStatuses = new List<FlowStatus>();
            var now = _utcNow();
            lock (_lock)
                foreach (var (_, status) in _dict)
                    if (status.Timeouts.HasExpiredTimeouts(now))
                        expiredStatuses.Add(status);

            foreach (var status in expiredStatuses)
                status.Timeouts.SignalExpiredTimeouts(now);

            await Task.Delay(10);
        }
    }

    public void Dispose() => _disposed = true;

    public void AddFlow(StoredId id, Action suspend, QueueManager queueManager, FlowTimeouts timeouts)
    {
        lock (_lock)
            _dict[id] = new FlowStatus(id, suspend, queueManager, Threads: 1, SuspendedThreads: 0, timeouts);
    }

    public void RemoveFlow(StoredId id)
    {
        lock (_lock)
            _dict.Remove(id);
    }

    public void Interrupt(IEnumerable<StoredId> ids)
    {
        lock (_lock)
        {
            foreach (var id in ids)
            {
                if (!_dict.ContainsKey(id))
                    continue;
                
                var queueManager = _dict[id].QueueManager;
                InterruptThreads(id);
                Task.Run(() => queueManager.FetchAndTryToDeliver());
            }
        }
    }

    public void StartThread(StoredId id)
    {
        lock (_lock)
        {
            if (!_dict.ContainsKey(id))
                return;

            var status = _dict[id];
            _dict[id] = status with { Threads = status.Threads + 1 };
        }
    }

    public void CompleteThread(StoredId id)
    {
        lock (_lock)
        {
            if (!_dict.ContainsKey(id))
                return;

            var status = _dict[id];
            _dict[id] = status with { Threads = status.Threads - 1 };
        }
    }

    public void InterruptThreads(StoredId id)
    {
        lock (_lock)
        {
            if (!_dict.ContainsKey(id))
                return;

            var status = _dict[id];
            _dict[id] = status with { SuspendedThreads = 0 };
        }
    }

    public void SuspendThread(StoredId id)
    {
        lock (_lock)
        {
            if (!_dict.ContainsKey(id))
                return;

            var status = _dict[id];
            _dict[id] = status with { SuspendedThreads = status.SuspendedThreads + 1 };
        }
    }

}