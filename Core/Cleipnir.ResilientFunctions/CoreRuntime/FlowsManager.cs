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
            return _dict[id] = new FlowExecutionState(id, subflows: 1, waitingSubflows: 0, timeouts, completed, _messageClearer);
    }

    public void RemoveFlow(StoredId id, FlowExecutionState flowExecutionState)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var existingState) && flowExecutionState == existingState)
              _dict.Remove(id);
    }

    public Task Push(IReadOnlyList<StoredMessages> messagesByFlow)
    {
        List<Task> tasks = new();
        List<StoredMessages> notLive = new();
        List<long> emptyPositionsForLiveFlows = new();
        lock (_lock)
            foreach (var storedMessages in messagesByFlow)
                if (_dict.TryGetValue(storedMessages.StoredId, out var flowState) && !flowState.Suspended)
                {
                    // Empty messages exist only to force a restart and carry nothing to deliver. The flow is live,
                    // so no restart is needed now - but the message may not be deleted either: the flow could be
                    // suspending concurrently, and the append's restart guarantee must survive that race. Reopen
                    // the positions instead, so the empty message is re-fetched and only consumed by an actual
                    // restart once the flow leaves the live set.
                    if (storedMessages.Messages.Any(message => message.IsEmpty))
                    {
                        emptyPositionsForLiveFlows.AddRange(
                            storedMessages.Messages.Where(message => message.IsEmpty).Select(message => message.Position)
                        );
                        var deliverable = storedMessages.Messages.Where(message => !message.IsEmpty).ToList();
                        if (deliverable.Count > 0)
                            tasks.Add(flowState.Push(deliverable));
                    }
                    else
                        tasks.Add(flowState.Push(storedMessages.Messages));
                }
                else
                    // Not in the dictionary, or a suspended entry lingering there (a suspended flow's parked
                    // invocation never reaches RemoveFlow by design) - restart the flow to deliver.
                    notLive.Add(storedMessages);

        if (emptyPositionsForLiveFlows.Count > 0)
            _messageClearer.ReopenPositions(emptyPositionsForLiveFlows);

        if (notLive.Count > 0)
            tasks.Add(RestartExecutions(notLive));

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

        Dictionary<StoredId, StoredFlowWithEffects> results;
        try
        {
            results = await _functionStore
                .RestartExecutionsWithoutMessages(groups.Keys.ToList(), _clusterInfo.ReplicaId);
        }
        catch
        {
            // The claim never happened, but the MessageWatchdog already marked the positions as pushed - reopen
            // them all so the messages are re-fetched and delivery is retried on a later poll.
            _messageClearer.ReopenPositions(
                groups.Values.SelectMany(g => g).SelectMany(sm => sm.Messages).Select(m => m.Position)
            );
            throw;
        }

        // Flows that could not be claimed were never delivered to, yet the MessageWatchdog optimistically marked
        // their positions as pushed. Their positions must leave the pushed-set again: parked per flow when the
        // flow has completed (kept in the ignore-set until an explicit re-invocation), reopened for re-fetch
        // otherwise.
        foreach (var (storedId, storedMessagesList) in groups.Where(kv => !results.ContainsKey(kv.Key)))
        {
            // Park BEFORE reading the status. An explicit re-invocation of a completed flow claims it and then
            // reopens its parked positions - so whichever side acts second sees the other's write: a park landing
            // before the restart's reopen is released by that reopen, while a park landing after it also lands
            // after the claim, making the status read below observe the no-longer-completed flow and release the
            // park. Checking the status first would leave a window where the restart's reopen misses a park based
            // on a stale completed-status read, stranding the positions in the ignore-set forever.
            _messageClearer.ParkPositions(
                storedId,
                storedMessagesList.SelectMany(sm => sm.Messages).Select(m => m.Position).ToList()
            );

            StoredFlow? storedFlow;
            try
            {
                storedFlow = await _functionStore.GetFunction(storedId);
            }
            catch
            {
                // Status unknown - release the park below so delivery is retried rather than the positions
                // being stranded.
                storedFlow = null;
            }

            if (storedFlow != null && storedFlow.Status is Status.Succeeded or Status.Failed)
                // Completed flows can never consume their messages - the positions stay parked (still in the
                // ignore-set, so no re-fetch churn) until an explicit re-invocation reopens them.
                continue;

            // The flow may become claimable later (executing elsewhere, a lost claim race, or a flow that has
            // not been created yet - messages may legally precede their flow): release the park so the messages
            // are re-fetched - without deleting them from the store.
            _messageClearer.ReopenParkedPositions(storedId);
        }

        // Resume each restarted flow, supplying the messages we already hold so it does not re-fetch them. Empty
        // messages exist only to force the restart, so they are excluded from delivery. The claim + flow snapshot
        // returned by RestartExecutionsWithoutMessages is everything the delegate needs, so no further store
        // round-trip or re-claim is performed.
        foreach (var (storedId, storedFlowWithEffects) in results)
        {
            var inHandMessages = groups[storedId]
                .SelectMany(storedMessages => storedMessages.Messages)
                .Where(message => !message.IsEmpty)
                .ToList();

            var restartedFunction = new RestartedFunction(
                storedFlowWithEffects.StoredFlow,
                storedFlowWithEffects.Effects,
                inHandMessages,
                storedFlowWithEffects.StorageSession
            );

            await _restarter!.ScheduleRestart(storedId, restartedFunction, onCompletion: () => { });
        }

        // The restarts the batch's empty messages were appended to force have now happened - delete them from
        // the store so they are not fetched and acted on again.
        var restartedEmptyPositions = results.Keys
            .SelectMany(storedId => groups[storedId])
            .SelectMany(storedMessages => storedMessages.Messages)
            .Where(message => message.IsEmpty)
            .Select(message => message.Position)
            .ToList();
        if (restartedEmptyPositions.Count > 0)
            await _messageClearer.Clear(restartedEmptyPositions);
    }
}
