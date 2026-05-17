using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager
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

    public IReadOnlyList<StoredId> FilterOwned(IEnumerable<StoredId> ids)
    {
        lock (_lock)
            return ids.Where(_dict.ContainsKey).ToList();
    }

    public Task Interrupt(IReadOnlyList<StoredId> ids)
    {
        /*
         * lock (_lock)
           foreach (var id in ids)
               if (_dict.TryGetValue(id, out var flowState))
                   flowState.Interrupt();

         */
        return Task.CompletedTask;
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
