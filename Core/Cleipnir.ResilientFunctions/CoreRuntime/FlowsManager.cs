using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

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

    public FlowExecutionState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed, TimeSpan maxWait)
    {
        lock (_lock)
            return _dict[id] = new FlowExecutionState(id, subflows: 1, waitingSubflows: 0, timeouts, completed, maxWait, _messageClearer);
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
                else if (flowState != null)
                    // The flow has decided to suspend but its persistence and tear-down are still in flight -
                    // wait for the invocation to complete, then restart it with the messages still in hand,
                    // instead of bouncing them through a position-reopen and a later watchdog poll.
                    tasks.Add(DeliverAfterSuspensionCompletes(flowState, storedMessages));
                else
                    // Not in the dictionary - restart the flow to deliver.
                    notLive.Add(storedMessages);

        if (emptyPositionsForLiveFlows.Count > 0)
            _messageClearer.ReopenPositions(emptyPositionsForLiveFlows);

        if (notLive.Count > 0)
            tasks.Add(RestartExecutions(notLive));

        return Task.WhenAll(tasks);
    }

    private async Task DeliverAfterSuspensionCompletes(FlowExecutionState flowState, StoredMessages storedMessages)
    {
        await flowState.Completed;
        await RestartExecutions([storedMessages]);
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
                .RestartExecutions(groups.Keys.ToList(), _clusterInfo.ReplicaId);
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
        // their positions as pushed. Completed flows can never consume their messages - inline them into the
        // flow's effect state (and delete the rows) so any later re-invocation, on any replica and via any
        // restart path, finds them in the effect snapshot the restart hands over. All other flows may become
        // claimable later (executing elsewhere, a lost claim race, or a flow that has not been created yet -
        // messages may legally precede their flow): reopen their positions so the messages are re-fetched.
        foreach (var (storedId, storedMessagesList) in groups.Where(kv => !results.ContainsKey(kv.Key)))
        {
            var flowMessages = storedMessagesList.SelectMany(sm => sm.Messages).ToList();

            StoredFlow? storedFlow;
            try
            {
                storedFlow = await _functionStore.GetFunction(storedId);
            }
            catch
            {
                // Status unknown - reopen below so delivery is retried rather than the positions being stranded.
                storedFlow = null;
            }

            if (storedFlow != null && storedFlow.Status is Status.Succeeded or Status.Failed)
                if (await TryInlinePendingMessages(storedId, flowMessages))
                    continue;

            _messageClearer.ReopenPositions(flowMessages.Select(m => m.Position));
        }

        // Resume each restarted flow, supplying the messages we already hold so it does not re-fetch them. Empty
        // messages exist only to force the restart, so they are excluded from delivery. The claim + flow snapshot
        // returned by RestartExecutions is everything the delegate needs, so no further store
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

    /// <summary>
    /// Persists a completed flow's in-hand messages into its effect state (the reserved pending-messages entry)
    /// and deletes them from the message store - empty restart-pokes are just deleted (a completed flow needs no
    /// restart). The effect write demands the flow is unowned (owner IS NULL) and that its effect version still
    /// matches the read the merged entry was computed from - the version is bumped by every claim and every
    /// unowned write, so a write that succeeds is provably based on the flow's current effect state: no claim ran
    /// in between (any later claim's snapshot therefore includes the entry) and no concurrently written effect is
    /// overwritten. After the verified write the status is re-read; if the flow has been resurrected in the
    /// meantime (e.g. via control-panel restart), false is returned so the caller reopens the positions and
    /// normal delivery takes over - the then-redundant entry is erased by the incarnation's own flushes or pruned
    /// on delivery. Returns true when the messages were inlined and their rows deleted.
    /// </summary>
    private async Task<bool> TryInlinePendingMessages(StoredId storedId, IReadOnlyList<StoredMessage> messages)
    {
        try
        {
            // Inline from the store's CURRENT rows, not the in-hand copies: control-panel tooling may have
            // replaced (stale content) or deleted (Clear/Remove) rows since the fetch - a deleted row must stay
            // deleted and a replaced row must be inlined with its fresh content. In-hand positions whose rows are
            // gone are still cleared below, which trims them from the ignore-set (the row delete is a no-op).
            var inHandPositions = messages.Where(m => !m.IsEmpty).Select(m => m.Position).ToHashSet();
            var currentRows = await _functionStore.MessageStore.GetMessages(storedId);
            var deliverable = currentRows.Where(m => !m.IsEmpty && inHandPositions.Contains(m.Position)).ToList();

            if (deliverable.Count > 0)
            {
                // Merge-write-verify loop: the version guard serializes unowned writers against each other and
                // against claims, so a conflicting write simply fails and the merge is retried from a fresh read.
                // The initial containment check doubles as the fast path when another writer already inlined this
                // batch's messages.
                var verified = false;
                for (var attempt = 0; attempt < 5 && !verified; attempt++)
                {
                    var storedFlowSnapshot = await _functionStore.GetFunction(storedId);
                    if (storedFlowSnapshot == null || storedFlowSnapshot.Status is not (Status.Succeeded or Status.Failed))
                        return false;

                    var effects = storedFlowSnapshot.Effects ?? [];
                    var byPosition = new Dictionary<long, StoredMessage>();
                    var existingEntry = effects.FirstOrDefault(e => e.EffectId == PendingMessages.EffectId);
                    if (existingEntry?.Result is { Length: > 0 } existingBytes)
                        foreach (var pending in PendingMessages.Decode(existingBytes))
                            byPosition[pending.Position] = pending;

                    if (deliverable.All(m => byPosition.ContainsKey(m.Position)))
                    {
                        verified = true;
                        continue;
                    }

                    foreach (var message in deliverable)
                        byPosition[message.Position] = message;

                    var session = new SnapshotStorageSession
                    {
                        Version = storedFlowSnapshot.Version
                    };
                    foreach (var effect in effects)
                        session.Effects[effect.EffectId] = effect;

                    var entry = StoredEffect.CreateCompleted(
                        PendingMessages.EffectId,
                        PendingMessages.Encode(byPosition.Values.OrderBy(m => m.Position).ToList()),
                        alias: null
                    );
                    try
                    {
                        await _functionStore.SetEffectResult(
                            storedId,
                            new StoredEffectChange(storedId, PendingMessages.EffectId, CrudOperation.Insert, entry),
                            owner: null,
                            session
                        );
                    }
                    catch (UnexpectedStateException)
                    {
                        // Version or owner guard failed - another writer or a claim got in between; retry from a
                        // fresh read.
                    }
                }

                if (!verified)
                    return false;
            }

            // Delete the rows only while the flow is still completed - otherwise keep them (caller reopens) and
            // let normal delivery handle them.
            var storedFlow = await _functionStore.GetFunction(storedId);
            if (storedFlow == null || storedFlow.Status is not (Status.Succeeded or Status.Failed))
                return false;

            await _messageClearer.Clear(messages.Select(m => m.Position).ToList());
            return true;
        }
        catch
        {
            // Includes the owner-guard's concurrent-modification signal (the flow was claimed mid-write) - the
            // caller reopens the positions and the next poll retries.
            return false;
        }
    }
}
