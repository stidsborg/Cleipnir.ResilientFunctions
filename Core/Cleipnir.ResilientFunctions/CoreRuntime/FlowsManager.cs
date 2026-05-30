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

    public FlowState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed)
    {
        lock (_lock)
            return _dict[id] = new FlowState(id, subflows: 1, waitingSubflows: 0, timeouts, completed);
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

    public void Interrupt(IReadOnlyList<StoredId> ids)
    {
        lock (_lock)
            foreach (var id in ids)
                if (_dict.TryGetValue(id, out var flowState))
                    flowState.Interrupt();
    }

    /*
    public async Task CheckForSuspension()
    {
        while (true)
        {
            var waitingFlows = new List<FlowState>();
            lock (_dict)
            {
                waitingFlows = _dict.Values.Where(s => s.)
                foreach (var flowState in _dict.Values)
                {
                    flowState.
                }
            }
            
            await Task.Delay(250);
        }
    }*/

}
