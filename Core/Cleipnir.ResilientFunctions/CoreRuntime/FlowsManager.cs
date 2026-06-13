using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager : IFlowsManager
{
    private readonly Dictionary<StoredId, FlowState> _dict = new();
    private readonly Dictionary<StoredType, IScheduleRestart> _scheduleRestarts = new();
    private readonly IFunctionStore _functionStore;
    private readonly Lock _lock = new();

    public FlowsManager(IFunctionStore functionStore) => _functionStore = functionStore;

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

    public void RegisterScheduleRestart(StoredType storedType, IScheduleRestart scheduleRestart)
    {
        lock (_lock)
            _scheduleRestarts[storedType] = scheduleRestart;
    }

    /// <summary>
    /// Wakes the target flow so it consumes the just-published message by applying the durable interrupt
    /// (interrupted flag + expires=0) - the suspend-race guard and watchdog backstop, so the message is never
    /// lost even when the target suspends concurrently or is owned by another replica. An idle target this
    /// replica executes is then picked up by the PostponedWatchdog.
    /// </summary>
    /// <remarks>
    /// STOPGAP: the immediate in-process restart of an idle target - look up <see cref="_scheduleRestarts"/>
    /// by <see cref="StoredId.Type"/> for a flow not in <c>_dict</c> and await
    /// <see cref="IScheduleRestart.ScheduleRestart"/> - is disabled. It races with the suspend/resume
    /// machinery and deadlocks the ping-pong / multi-flow message exchange. The durable interrupt below still
    /// guarantees delivery via the PostponedWatchdog. Restore the immediate restart once the suspension rework
    /// lands. <see cref="RegisterScheduleRestart"/> keeps populating <see cref="_scheduleRestarts"/> so that
    /// re-enabling is a local change here.
    /// </remarks>
    public Task Schedule(StoredId storedId)
        => _functionStore.Interrupt(storedId);
}
