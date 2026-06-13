using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

/// <summary>
/// Maps a flow type to the FlowsManager responsible for executing it, so a message publisher can ask the
/// right manager to schedule the target flow for immediate execution.
/// </summary>
public class FlowsManagerRegistry : IFlowsManagerRegistry
{
    private readonly Dictionary<StoredType, FlowsManager> _flowsManagers = new();
    private readonly IFunctionStore _functionStore;
    private readonly Lock _lock = new();

    public FlowsManagerRegistry(IFunctionStore functionStore) => _functionStore = functionStore;

    public void Register(StoredType storedType, FlowsManager flowsManager)
    {
        lock (_lock)
            _flowsManagers[storedType] = flowsManager;
    }

    /// <summary>
    /// Wakes the target flow so it consumes the just-published message.
    /// Always applies the durable interrupt (interrupted flag + expires=0) - this is the suspend-race guard
    /// and the watchdog backstop, so the message is never lost even when the target suspends concurrently or
    /// is owned by another replica. When this process executes the flow's type, the responsible FlowsManager
    /// additionally restarts an idle target immediately instead of waiting for the PostponedWatchdog tick.
    /// </summary>
    public async Task Schedule(StoredId storedId)
    {
        await _functionStore.Interrupt(storedId);

        FlowsManager? flowsManager;
        lock (_lock)
            _flowsManagers.TryGetValue(storedId.Type, out flowsManager);
        flowsManager?.Schedule(storedId);
    }
}
