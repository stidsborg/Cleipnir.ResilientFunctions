using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowTimeouts
{
    private readonly FlowsTimeoutManager? _flowsTimeoutManager;
    private readonly StoredId? _storedId;
    private readonly Lock _lock = new();
    private Dictionary<EffectId, DateTime> Timeouts { get; } = new();

    public FlowTimeouts() { }

    public FlowTimeouts(FlowsTimeoutManager flowsTimeoutManager, StoredId storedId)
    {
        _flowsTimeoutManager = flowsTimeoutManager;
        _storedId = storedId;
    }

    public DateTime? MinimumTimeout
    {
        get
        {
            lock (_lock)
                return Timeouts.Values.Count != 0 ? Timeouts.Values.Min() : null;
        }
    }

    public void AddTimeout(EffectId effectId, DateTime timeout)
    {
        lock (_lock)
            Timeouts[effectId] = timeout;

        _flowsTimeoutManager?.RegisterTimeout(_storedId!, effectId, timeout);
    }

    public void RemoveTimeout(EffectId effectId)
    {
        lock (_lock)
            Timeouts.Remove(effectId);

        _flowsTimeoutManager?.RemoveTimeout(_storedId!, effectId);
    }
}