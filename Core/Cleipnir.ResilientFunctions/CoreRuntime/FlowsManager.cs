using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowsManager
{
    private readonly Dictionary<StoredId, FlowState> _dict = new();
    private readonly Lock _lock = new();

    private readonly UtcNow _utcNow;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal FlowsManager(UtcNow? utcNow = null, ShutdownCoordinator? shutdownCoordinator = null, UnhandledExceptionHandler? unhandledExceptionHandler = null)
    {
        _utcNow = utcNow!;
        _shutdownCoordinator = shutdownCoordinator!;
        _unhandledExceptionHandler = unhandledExceptionHandler!;
    }

    public FlowState CreateFlowState(StoredId id, FlowTimeouts timeouts, Task completed, TimeSpan maxWait)
    {
        lock (_lock)
            return _dict[id] = new FlowState(id, subflows: 1, timeouts, completed, maxWait, _utcNow);
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

    public IReadOnlyList<StoredId> Interrupt(IReadOnlyList<StoredId> ids)
    {
        var interrupted = new List<StoredId>();
        lock (_lock)
            foreach (var id in ids)
                if (_dict.TryGetValue(id, out var flowState))
                    if (flowState.Interrupt())
                        interrupted.Add(id);

        return interrupted;
    }

    public bool TrySuspend(FlowState state)
    {
        var suspend = state.TrySuspend();
        if (suspend)
            lock (_lock)
                _dict.Remove(state.Id);

        return suspend;
    }
    
    public async Task CheckForSuspension()
    {
        while (!_shutdownCoordinator.ShutdownInitiated)
        {
            try
            {
                List<FlowState> waiting;
                lock (_lock)
                    waiting = _dict.Values.ToList();

                foreach (var flowState in waiting)
                    if (flowState.SuspendIfMaxWaitExceeded())
                        lock (_lock)
                            _dict.Remove(flowState.Id);
            }
            catch (Exception thrownException)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(FlowsManager)} suspension check failed",
                        innerException: thrownException
                    )
                );
            }

            await Task.Delay(250);
        }
    }
}
