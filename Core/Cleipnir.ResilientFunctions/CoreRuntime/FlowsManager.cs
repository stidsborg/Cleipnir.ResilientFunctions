using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager
{
    private readonly Dictionary<StoredId, FlowExecutionState> _dict = new();
    private readonly IFunctionStore _functionStore;
    private readonly IMessageClearer _messageClearer;
    private readonly ClusterInfo _clusterInfo;
    private readonly IFlowRestarter _restarter;
    private readonly Lock _lock = new();

    internal FlowsManager(
        IFunctionStore functionStore,
        IMessageClearer messageClearer,
        ClusterInfo clusterInfo,
        IFlowRestarter restarter)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
        _restarter = restarter;
    }

    public FlowExecutionState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed, TimeSpan maxWait)
        => new(id, subflows: 1, waitingSubflows: 0, timeouts, completed, maxWait);

    /// <summary>
    /// Registers the flow as live so pushes are routed to it. Called as the final preparation step - after the
    /// queue manager has been attached - so a flow reachable through the dictionary always has one.
    /// </summary>
    public void AddFlow(FlowExecutionState flowExecutionState)
    {
        lock (_lock)
            _dict[flowExecutionState.Id] = flowExecutionState;
    }

    public void RemoveFlow(StoredId id, FlowExecutionState flowExecutionState)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var existingState) && flowExecutionState == existingState)
              _dict.Remove(id);
    }

    internal Task Push(IReadOnlyList<IncomingMessage> messages)
    {
        List<Task> tasks = new();
        List<IncomingMessage> notLive = new();
        List<long> emptyPositionsForLiveFlows = new();
        lock (_lock)
            // The first point that genuinely needs per-flow batches - the pipeline is flat until here.
            foreach (var flowGroup in messages.GroupBy(message => message.StoredId))
            {
                if (!_dict.TryGetValue(flowGroup.Key, out var flowState))
                {
                    // Not in the dictionary - restart the flow to deliver.
                    notLive.AddRange(flowGroup);
                    continue;
                }

                var flowMessages = flowGroup.ToList();

                // Empty messages exist only to force a restart and carry nothing to deliver. The flow is live,
                // so no restart is needed now - but the message may not be deleted either: the flow could be
                // suspending concurrently, and the append's restart guarantee must survive that race. Reopen
                // the positions instead, so the empty message is re-fetched and only consumed by an actual
                // restart once the flow leaves the live set.
                if (!flowState.Suspended && flowMessages.Any(message => message.IsEmpty))
                {
                    emptyPositionsForLiveFlows.AddRange(
                        flowMessages.Where(message => message.IsEmpty).Select(message => message.Position!.Value)
                    );
                    var deliverable = flowMessages.Where(message => !message.IsEmpty).ToList();
                    if (deliverable.Count > 0)
                        tasks.Add(DeliverToFlow(flowState, deliverable));
                }
                else
                    tasks.Add(DeliverToFlow(flowState, flowMessages));
            }

        if (emptyPositionsForLiveFlows.Count > 0)
            _messageClearer.ReopenPositions(emptyPositionsForLiveFlows);

        if (notLive.Count > 0)
            tasks.Add(RestartExecutions(notLive));

        return Task.WhenAll(tasks);
    }

    // Delivers to the live flow - unless it no longer accepts pushes (it has decided to suspend or its
    // invocation is ending), in which case the delivery waits for the invocation to complete (the final status
    // is persisted by then) and restarts the flow with the messages still in hand, instead of bouncing them
    // through a position-reopen and a later watchdog poll. A suspended flow's batch may still contain empty
    // messages: the refusal happens at the push's entry, so they never reach the queue manager and ride to the
    // restart, which consumes them.
    private async Task DeliverToFlow(FlowExecutionState flowState, IReadOnlyList<IncomingMessage> messages)
    {
        var undelivered = await flowState.Push(messages);
        if (undelivered is null)
            return;

        await flowState.Completed;
        await RestartExecutions(undelivered);
    }

    /// <summary>
    /// Restarts (claims for this replica) the targeted flows that are not already owned, then hands each restarted
    /// flow - together with the in-hand messages - to the <see cref="ScheduleRestartFromWatchdog"/> delegate so it
    /// resumes executing. Flows that could not be claimed have their positions reopened in the message clearer
    /// (dropped from the ignore-set without deleting them from the store, since their actual owner still needs them).
    /// </summary>
    internal async Task RestartExecutions(IReadOnlyList<IncomingMessage> messages)
    {
        var groups = messages
            .GroupBy(m => m.StoredId)
            .ToDictionary(g => g.Key, g => g.ToList());

        var results = await _functionStore
            .RestartExecutions(groups.Keys.ToList(), _clusterInfo.ReplicaId);

        // Flows that could not be claimed were never delivered to, yet the MessageWatchdog optimistically marked
        // their positions as pushed. Completed flows can never consume their messages - dead letter them (and
        // delete the rows) so they are parked for inspection and explicit redrive instead of being re-fetched
        // forever. All other flows may become claimable later (executing elsewhere, a lost claim race, or a flow
        // that has not been created yet - messages may legally precede their flow): reopen their positions so
        // the messages are re-fetched.
        foreach (var (storedId, flowMessages) in groups.Where(kv => !results.ContainsKey(kv.Key)))
        {
            // Fetched messages always address a store row, so every position is present.
            var deliverablePositions = flowMessages.Where(m => !m.IsEmpty).Select(m => m.Position!.Value).ToList();
            var allPositions = flowMessages.Select(m => m.Position!.Value).ToList();

            var storedFlow = await _functionStore.GetFunction(storedId);
            if (storedFlow != null && storedFlow.Status is Status.Succeeded or Status.Failed)
                if (await TryDeadLetterMessages(storedId, deliverablePositions, allPositions))
                    continue;

            _messageClearer.ReopenPositions(allPositions);
        }

        // Resume each restarted flow, supplying the messages we already hold so it does not re-fetch them. Empty
        // messages exist only to force the restart, so they are excluded from delivery. The claim + flow snapshot
        // returned by RestartExecutions is everything the delegate needs, so no further store
        // round-trip or re-claim is performed.
        foreach (var (storedId, storedFlowWithEffects) in results)
        {
            var inHandMessages = groups[storedId]
                .Where(message => !message.IsEmpty)
                .ToList();

            var restartedFunction = new RestartedFunction(
                storedFlowWithEffects.StoredFlow,
                storedFlowWithEffects.Effects,
                inHandMessages,
                storedFlowWithEffects.StorageSession
            );

            await _restarter.ScheduleRestart(storedId, restartedFunction, onCompletion: () => { });
        }

        // The restarts the batch's empty messages were appended to force have now happened - delete them from
        // the store so they are not fetched and acted on again.
        var restartedEmptyPositions = results.Keys
            .SelectMany(storedId => groups[storedId])
            .Where(message => message.IsEmpty)
            .Select(message => message.Position!.Value)
            .ToList();
        if (restartedEmptyPositions.Count > 0)
            await _messageClearer.Clear(restartedEmptyPositions);
    }

    /// <summary>
    /// Moves a completed flow's in-hand messages to the dead letter queue and deletes them from the message
    /// store - empty restart-pokes are just deleted (a completed flow needs no restart). The dlq append happens
    /// before the row delete, so a crash in between dead letters the messages a second time rather than losing
    /// them. Returns true when the messages were dead lettered and their rows deleted; on a store failure false
    /// is returned so the caller reopens the positions and a later poll retries.
    /// </summary>
    private async Task<bool> TryDeadLetterMessages(StoredId storedId, IReadOnlyList<long> deliverablePositions, IReadOnlyList<long> allPositions)
    {
        try
        {
            // Dead letter the store's CURRENT rows, not the in-hand copies: control-panel tooling may have
            // replaced (stale content) or deleted (Clear/Remove) rows since the fetch - a deleted row must stay
            // deleted and a replaced row must be dead lettered with its fresh content. In-hand positions whose
            // rows are gone are still cleared below, which trims them from the ignore-set (the row delete is a
            // no-op).
            var inHandPositions = deliverablePositions.ToHashSet();
            var currentRows = await _functionStore.MessageStore.GetMessages(storedId);
            var deliverable = currentRows.Where(m => !m.IsEmpty && inHandPositions.Contains(m.Position)).ToList();

            if (deliverable.Count > 0)
                await _functionStore.DlqStore.Append(deliverable);

            await _messageClearer.Clear(allPositions);
            return true;
        }
        catch
        {
            // The caller reopens the positions and the next poll retries - at worst a batch whose dlq append
            // landed before the failure is dead lettered a second time rather than lost.
            return false;
        }
    }
}
