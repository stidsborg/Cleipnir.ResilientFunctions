using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager
{
    private readonly Dictionary<StoredId, FlowState> _dict = new();
    private readonly Dictionary<StoredType, Func<StoredId, Task>> _scheduleRestarts = new();
    private readonly HashSet<StoredId> _scheduling = new();
    private readonly Lock _lock = new();

    public FlowState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed)
    {
        lock (_lock)
            return _dict[id] = new FlowState(id, subflows: 1, waitingSubflows: 0, timeouts, completed);
    }

    public void RemoveFlow(StoredId id, FlowState flowState)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var existingState) && flowState == existingState)
              _dict.Remove(id);
    }

    public IReadOnlyList<StoredId> FilterOwned(IEnumerable<StoredId> ids)
    {
        lock (_lock)
            return ids.Where(_dict.ContainsKey).ToList();
    }

    public void Interrupt(IReadOnlyList<StoredId> ids)
    {
        lock (_lock)
            foreach (var id in ids)
                if (_dict.TryGetValue(id, out var flowState))
                    flowState.Interrupt();
    }

    public Task Push(IReadOnlyDictionary<StoredId, List<StoredMessage>> messagesByFlow)
    {
        List<Task> tasks = new();
        lock (_lock)
            foreach (var (id, messages) in messagesByFlow)
                if (_dict.TryGetValue(id, out var flowState))
                    tasks.Add(flowState.Push(messages));

        return Task.WhenAll(tasks);
    }

    public void RegisterScheduleRestart(StoredType storedType, Func<StoredId, Task> scheduleRestart)
    {
        lock (_lock)
            _scheduleRestarts[storedType] = scheduleRestart;
    }

    /// <summary>
    /// Schedules the flow for immediate execution. No-op when the flow is already executing on this
    /// replica (it receives the message through the push pipeline) or when this manager does not execute
    /// the flow's type.
    /// </summary>
    public void Schedule(StoredId storedId)
    {
        Func<StoredId, Task> scheduleRestart;
        lock (_lock)
        {
            if (_dict.ContainsKey(storedId))
                return; // already executing on this replica
            if (_scheduling.Contains(storedId))
                return; // a restart is already in flight - avoid a restart storm on replayed publishes
            if (!_scheduleRestarts.TryGetValue(storedId.Type, out var restart))
                return; // this manager does not execute the flow's type
            scheduleRestart = restart;
            _scheduling.Add(storedId);
        }

        // Fire-and-forget: the publisher must not block on (and re-enter through) the target's restart.
        // The durable interrupt already guarantees the message is not lost; this just restarts it sooner.
        _ = Task.Run(async () =>
        {
            try
            {
                await scheduleRestart(storedId);
            }
            catch (UnexpectedStateException)
            {
                // the flow was concurrently restarted / owned elsewhere - nothing to do
            }
            finally
            {
                lock (_lock)
                    _scheduling.Remove(storedId);
            }
        });
    }

    /*
    public async Task CheckForSuspension()
    {
        while (true)
        {
            var waitingFlows = new List<FlowState>();
            lock (_dict)
            {
                waitingFlows = _dict.Values.Where(s => s.)
                foreach (var flowState in _dict.Values)
                {
                    flowState.
                }
            }
            
            await Task.Delay(250);
        }
    }*/

}
