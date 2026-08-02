using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

/// <summary>
/// Holds one <see cref="FlowsManager"/> per <see cref="StoredType"/> so each manager is concerned with a single
/// flow type only. Registration obtains its type's manager through <see cref="GetOrCreate"/>; the watchdogs call
/// the routing methods below, which group the incoming ids by <see cref="StoredId.Type"/> and dispatch to the
/// matching per-type manager. Ids for types not registered on this replica are held for a grace period and then
/// dead lettered.
/// </summary>
public class FlowsManagers
{
    private readonly Dictionary<StoredType, FlowsManager> _managers = new();

    private readonly IFunctionStore _functionStore;
    private readonly MessageClearer _messageClearer;
    private readonly ClusterInfo _clusterInfo;
    private readonly DlqManager _dlqManager;

    private readonly Lock _lock = new();

    internal FlowsManagers(
        IFunctionStore functionStore,
        MessageClearer messageClearer,
        ClusterInfo clusterInfo,
        DlqManager dlqManager)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
        _dlqManager = dlqManager;
    }

    public FlowsManager GetOrCreate(StoredType storedType)
    {
        lock (_lock)
        {
            if (_managers.TryGetValue(storedType, out var existing))
                return existing;

            return _managers[storedType] = new FlowsManager(_functionStore, _messageClearer, _clusterInfo);
        }
    }

    private FlowsManager? TryGet(StoredType storedType)
    {
        lock (_lock)
            return _managers.GetValueOrDefault(storedType);
    }

    public Task Push(IReadOnlyList<StoredMessage> messages)
    {
        List<Task> messageDeliveries;
        List<StoredMessage> unregistered;
        lock (_lock)
        {
            unregistered = messages
                .Where(msg => !_managers.ContainsKey(msg.StoredId.Type))
                .ToList();

            var running = messages
                .Where(msg => _managers.ContainsKey(msg.StoredId.Type))
                .GroupBy(msg => msg.StoredId.Type)
                .Select(g => _managers[g.Key].Push(g.ToList()))
                .ToList();

            messageDeliveries = running;
        }

        // Messages for flow types not registered on this replica can never be delivered here - flow types are
        // only registered at registry-creation time. Their positions stay marked as pushed (so they are not
        // re-fetched) while the DlqManager holds them and then dead letters them once the grace period expires.
        // The grace period gives a rolling deployment time to recycle this replica: a process restart discards
        // the in-memory hold, after which the messages are re-assigned to a replica that may have the type
        // registered.
        if (unregistered.Count > 0)
            _dlqManager.MoveToDlqAfterGracePeriod(unregistered);

        return Task.WhenAll(messageDeliveries);
    }
}
