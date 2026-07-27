using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
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
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly Lock _lock = new();

    internal FlowsManagers(
        IFunctionStore functionStore,
        MessageClearer messageClearer,
        ClusterInfo clusterInfo,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
        _unhandledExceptionHandler = unhandledExceptionHandler;
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

    /// <summary>
    /// Routes each flow's fetched messages to its type's manager. Returns the positions that were not handled,
    /// for the MessageWatchdog to reopen so delivery is retried on a later poll (see
    /// <see cref="FlowsManager.Push"/>). Never throws: a failed delivery leaves its positions neither cleared
    /// (handled terminally) nor returned, so the failure is reported and the entire batch returned for retry
    /// instead - over-reopening is safe, as re-pushes are idempotent (deduped by position) and a terminally
    /// handled message's row is already deleted, so its re-fetch finds nothing.
    /// </summary>
    public async Task<IReadOnlyList<long>> Push(IReadOnlyList<StoredMessages> messages)
    {
        List<Task<IReadOnlyList<long>>> messageDeliveries;
        List<StoredMessages> unregistered;
        lock (_lock)
        {
            unregistered = messages
                .Where(msg => !_managers.ContainsKey(msg.StoredId.Type))
                .ToList();

            messageDeliveries = messages
                .Where(msg => _managers.ContainsKey(msg.StoredId.Type))
                .GroupBy(msg => msg.StoredId.Type)
                .Select(g => _managers[g.Key].Push(g.ToList()))
                .ToList();
        }

        // Messages for flow types not (yet) registered on this replica cannot be delivered here - the type may
        // simply not have been registered yet (start-up ordering or a rolling deployment).
        // todo log a warning here
        var toReopen = unregistered
            .SelectMany(sm => sm.Messages)
            .Select(m => m.Position)
            .ToList();

        try
        {
            foreach (var positions in await Task.WhenAll(messageDeliveries))
                toReopen.AddRange(positions);
        }
        catch (Exception exception)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException("Message delivery failed - the batch is retried on a later poll", exception)
            );
            return messages.SelectMany(sm => sm.Messages).Select(m => m.Position).ToList();
        }

        return toReopen;
    }

}
