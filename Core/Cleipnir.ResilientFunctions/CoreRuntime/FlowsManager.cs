using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager : IDisposable
{
    private readonly Dictionary<StoredId, FlowState> _dict = new();
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
            var expiredStatuses = new List<FlowState>();
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
            _dict[id] = new FlowState(id, suspend, queueManager, threads: 1, suspendedThreads: 0, timeouts);
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
                if (!_dict.TryGetValue(id, out var flowState))
                    continue;

                flowState.Interrupt();
                Task.Run(() => flowState.QueueManager.FetchMessagesOnce());
            }

        }
    }

    public void StartThread(StoredId id)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var flowState))
                flowState.NewThreadStarted();
    }

    public void CompleteThread(StoredId id)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var flowState))
                flowState.ThreadCompleted();
    }

    public async Task SuspendThread(StoredId id)
    {
        FlowState? flowState;
        lock (_lock)
            _dict.TryGetValue(id, out flowState);

        if (flowState != null)
            await flowState.ThreadSuspended();
    }

    public Task GetInterruptedSignal(StoredId id)
    {
        lock (_lock)
            return _dict.TryGetValue(id, out var flowState)
                ? flowState.Signal.Wait()
                : Task.CompletedTask;
    }

    public void SignalInterrupt(StoredId id)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var flowState))
                flowState.Signal.Fire();
    }

    [DoesNotReturn]
    public async Task Suspend(StoredId id) => throw new SuspendInvocationException();

}