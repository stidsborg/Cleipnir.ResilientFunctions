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
/// matching per-type manager. Ids for types not registered on this replica are ignored.
/// </summary>
public class FlowsManagers
{
    private readonly Dictionary<StoredType, FlowsManager> _managers = new();
    
    private readonly IFunctionStore _functionStore;
    private readonly MessageClearer _messageClearer;
    private readonly ClusterInfo _clusterInfo;

    private readonly Lock _lock = new();

    internal FlowsManagers(IFunctionStore functionStore, MessageClearer messageClearer, ClusterInfo clusterInfo)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
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

    public Task Push(IReadOnlyList<StoredMessages> messages)
    {
        List<Task> messageDeliveries;
        List<StoredMessages> unregistered;
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

        // Messages for flow types not (yet) registered on this replica cannot be delivered here. Reopen their
        // positions so delivery is retried on a later poll - the type may simply not have been registered yet
        // (start-up ordering or a rolling deployment).
        if (unregistered.Count > 0)
            _messageClearer.ReopenPositions(
                unregistered.SelectMany(sm => sm.Messages).Select(m => m.Position)
            );

        return Task.WhenAll(messageDeliveries);
    }

}
