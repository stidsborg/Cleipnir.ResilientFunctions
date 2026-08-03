using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

/// <summary>
/// Holds one <see cref="FlowsManager"/> per <see cref="StoredType"/> so each manager is concerned with a single
/// flow type only. The MessageWatchdog consults <see cref="IsRegistered"/> at the fetch boundary - messages for
/// types not registered on this replica are held for the dlq grace period instead of being pushed. Registration
/// creates its type's manager through <see cref="Create"/>; <see cref="Push"/> groups the
/// already-deserialized batches by <see cref="StoredId.Type"/> and dispatches to the matching per-type manager.
///
/// The dictionary is unsynchronized because it is write-once-then-read-only: every <see cref="Create"/> call
/// happens in the setup delegate FunctionsRegistry.CreateAndStart runs before it starts - and seals - the registry,
/// so all inserts complete before the MessageWatchdog that reads it is ever launched.
/// </summary>
public class FlowsManagers
{
    private readonly Dictionary<StoredType, FlowsManager> _managers = new();

    private readonly IFunctionStore _functionStore;
    private readonly MessageClearer _messageClearer;
    private readonly ClusterInfo _clusterInfo;

    internal FlowsManagers(
        IFunctionStore functionStore,
        MessageClearer messageClearer,
        ClusterInfo clusterInfo)
    {
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _clusterInfo = clusterInfo;
    }

    /// <summary>
    /// Creates the type's manager - called once per flow type, as registering an already registered type returns
    /// the existing registration before reaching here.
    /// </summary>
    internal FlowsManager Create(StoredType storedType, IFlowRestarter restarter)
        => _managers[storedType] = new FlowsManager(_functionStore, _messageClearer, _clusterInfo, restarter);

    /// <summary>
    /// The type's manager. Resolved on use rather than handed to the Invoker at construction because the Invoker
    /// is the manager's restarter - it is created first and the manager immediately after.
    /// </summary>
    internal FlowsManager Get(StoredType storedType) => _managers[storedType];

    /// <summary>
    /// True when the flow type is registered on this replica.
    /// </summary>
    internal bool IsRegistered(StoredType storedType) => _managers.ContainsKey(storedType);

    // Every message here belongs to a registered type: the MessageWatchdog checked IsRegistered - against this
    // same dictionary - before deserializing, so the manager lookup cannot miss.
    internal Task Push(IReadOnlyList<IncomingMessage> messages)
    {
        var registered = messages
            .GroupBy(msg => msg.StoredId.Type)
            .Select(g => (Manager: _managers[g.Key], Messages: g.ToList()));

        return Task.WhenAll(registered.Select(pair => pair.Manager.Push(pair.Messages)));
    }
}
