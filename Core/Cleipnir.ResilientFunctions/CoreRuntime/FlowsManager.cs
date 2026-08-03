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

                tasks.Add(DeliverToFlow(flowState, flowGroup.ToList()));
            }

        if (notLive.Count > 0)
            tasks.Add(RestartExecutions(notLive));

        return Task.WhenAll(tasks);
    }

    // Delivers to the live flow - unless it no longer accepts pushes (it has decided to suspend or its
    // invocation is ending), in which case the delivery waits for the invocation to complete (the final status
    // is persisted by then) and restarts the flow with the messages still in hand, instead of bouncing them
    // through a position-reopen and a later watchdog poll. The batch may contain empty (restart-poke) messages:
    // on a refused push they ride to the restart, which consumes them, while on an accepted push the queue
    // manager reopens them, so they are re-fetched and consumed by a restart once the flow leaves the live set.
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
        var unclaimed = groups.Where(kv => !results.ContainsKey(kv.Key)).ToList();
        if (unclaimed.Count > 0)
        {
            // A single batched status read for all unclaimed flows - flows without a row are simply absent from
            // the result (they may be created later), which is the not-completed case anyway.
            var completed = (await _functionStore.GetFunctionsStatus(unclaimed.Select(kv => kv.Key)))
                .Where(s => s.Status is Status.Succeeded or Status.Failed)
                .Select(s => s.StoredId)
                .ToHashSet();

            foreach (var (storedId, flowMessages) in unclaimed)
            {
                // Fetched messages always address a store row, so every position is present.
                var positions = flowMessages.Select(m => m.Position!.Value).ToList();

                if (completed.Contains(storedId))
                {
                    await DeadLetterMessages(storedId, positions);
                    continue;
                }

                _messageClearer.ReopenPositions(positions);
            }
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
    /// them. A store failure simply propagates - the watchdog reports it and retries the poll, at worst dead
    /// lettering a batch whose append had already landed a second time rather than losing it.
    /// </summary>
    private async Task DeadLetterMessages(StoredId storedId, IReadOnlyList<long> positions)
    {
        // Dead letter the store's CURRENT rows, not the in-hand copies: control-panel tooling may have
        // replaced (stale content) or deleted (Clear/Remove) rows since the fetch - a deleted row must stay
        // deleted and a replaced row must be dead lettered with its fresh content. In-hand positions whose
        // rows are gone are still cleared below, which trims them from the ignore-set (the row delete is a
        // no-op).
        var inHandPositions = positions.ToHashSet();
        var currentRows = await _functionStore.MessageStore.GetMessages(storedId);
        var deliverable = currentRows.Where(m => !m.IsEmpty && inHandPositions.Contains(m.Position)).ToList();

        if (deliverable.Count > 0)
            await _functionStore.DlqStore.Append(deliverable);

        await _messageClearer.Clear(positions);
    }
}
