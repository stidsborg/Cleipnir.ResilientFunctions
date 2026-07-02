using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager
{
    private readonly Dictionary<StoredId, FlowExecutionState> _dict = new();
    private readonly IFunctionStore _functionStore;
    private readonly IMessageClearer _messageClearer;
    private readonly ClusterInfo _clusterInfo;
    private IFlowRestarter? _restarter;
    private readonly Lock _lock = new();

    internal FlowsManager(
        IFunctionStore functionStore,
        IMessageClearer messageClearer,
        ClusterInfo clusterInfo)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
    }

    /// <summary>
    /// Supplies the per-type flow restarter (the Invoker). Late-bound because the Invoker is constructed after this
    /// manager during registration.
    /// </summary>
    internal void SetRestarter(IFlowRestarter restarter) => _restarter = restarter;

    public FlowExecutionState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed)
    {
        lock (_lock)
            return _dict[id] = new FlowExecutionState(id, subflows: 1, waitingSubflows: 0, timeouts, completed, _messageClearer.ReopenPositions);
    }

    public void RemoveFlow(StoredId id, FlowExecutionState flowExecutionState)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var existingState) && flowExecutionState == existingState)
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

    public Task Push(IReadOnlyList<StoredMessages> messagesByFlow)
    {
        List<Task> tasks = new();
        List<StoredMessages> notLive = new();
        lock (_lock)
            foreach (var storedMessages in messagesByFlow)
                if (_dict.TryGetValue(storedMessages.StoredId, out var flowState))
                    tasks.Add(flowState.Push(storedMessages.Messages));
                else
                    notLive.Add(storedMessages);

        // The MessageWatchdog optimistically marked all pushed positions; a flow that is not live cannot receive
        // its messages now, so reopen the positions - stranding them in the ignore-set would let a later-positioned
        // duplicate consume the idempotency key first once the flow starts.
        if (notLive.Count > 0)
            _messageClearer.ReopenPositions(
                notLive.SelectMany(storedMessages => storedMessages.Messages).Select(message => message.Position)
            );

        return Task.WhenAll(tasks);
    }

    /// <summary>
    /// Restarts (claims for this replica) the targeted flows that are not already owned, then hands each restarted
    /// flow - together with the in-hand messages - to the <see cref="ScheduleRestartFromWatchdog"/> delegate so it
    /// resumes executing. Flows that could not be claimed have their positions reopened in the message clearer
    /// (dropped from the ignore-set without deleting them from the store, since their actual owner still needs them).
    /// </summary>
    public async Task RestartExecutions(IEnumerable<StoredMessages> messages)
    {
        var groups = messages
            .GroupBy(m => m.StoredId)
            .ToDictionary(g => g.Key, g => g.ToList());

        var results = await _functionStore
            .RestartExecutionsWithoutMessages(groups.Keys.ToList(), _clusterInfo.ReplicaId);

        // Flows that could not be restarted (already owned/running elsewhere, or no longer present) were never
        // delivered to here, yet the MessageWatchdog optimistically marked their positions as pushed. Drop those
        // positions from the clearer's ignore-set so they can be re-fetched - without deleting them from the store,
        // since the actual owner still needs them.
        var notRestartedPositions = groups
            .Where(kv => !results.ContainsKey(kv.Key))
            .SelectMany(kv => kv.Value)
            .SelectMany(storedMessages => storedMessages.Messages)
            .Select(message => message.Position)
            .ToList();
        _messageClearer.ReopenPositions(notRestartedPositions);

        // Resume each restarted flow, supplying the messages we already hold so it does not re-fetch them. The
        // claim + flow snapshot returned by RestartExecutionsWithoutMessages is everything the delegate needs, so
        // no further store round-trip or re-claim is performed.
        foreach (var (storedId, storedFlowWithEffects) in results)
        {
            var inHandMessages = groups[storedId]
                .SelectMany(storedMessages => storedMessages.Messages)
                .ToList();

            var restartedFunction = new RestartedFunction(
                storedFlowWithEffects.StoredFlow,
                storedFlowWithEffects.Effects,
                inHandMessages,
                storedFlowWithEffects.StorageSession
            );

            await _restarter!.ScheduleRestart(storedId, restartedFunction, onCompletion: () => { });
        }
    }

    /// <summary>
    /// Wakes the target flow so it consumes the just-published message by applying the durable interrupt
    /// (interrupted flag + expires=0) - the suspend-race guard and watchdog backstop, so the message is never
    /// lost even when the target suspends concurrently or is owned by another replica. An idle target this
    /// replica executes is then picked up by the PostponedWatchdog.
    /// </summary>
    public Task Schedule(StoredId storedId)
        => _functionStore.Interrupt(storedId);
}
