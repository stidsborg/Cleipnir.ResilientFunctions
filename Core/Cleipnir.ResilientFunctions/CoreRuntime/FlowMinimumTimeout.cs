using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowMinimumTimeout
{
    private readonly Lock _lock = new();
    private Dictionary<EffectId, DateTime> Timeouts { get; } = new();
    
    public DateTime? Current
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
    }

    public void RemoveTimeout(EffectId effectId)
    {
        lock (_lock)
            Timeouts.Remove(effectId);
    }
}