using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager(IFunctionStore functionStore)
{
    private readonly Dictionary<StoredId, FlowState> _dict = new();
    private readonly Lock _lock = new();

    public FlowState CreateFlow(StoredId id, FlowTimeouts timeouts)
    {
        lock (_lock)
            return _dict[id] = new FlowState(id, subflows: 1, waitingSubflows: 0, timeouts);;
    }

    public void RemoveFlow(StoredId id, FlowState flowState)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var existingState) && flowState == existingState)
              _dict.Remove(id);
    }

    public async Task Interrupt(IReadOnlyList<StoredId> ids)
    {
        await functionStore.ResetInterrupted(ids);

        lock (_lock)
            foreach (var id in ids)
                if (_dict.TryGetValue(id, out var flowState))
                    flowState.Interrupt();
    }

    public void StartThread(StoredId id)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var flowState))
                flowState.SubflowStarted();
    }

    public void CompleteThread(StoredId id)
    {
        lock (_lock)
            if (_dict.TryGetValue(id, out var flowState))
                flowState.SubflowCompleted();
    }
}
