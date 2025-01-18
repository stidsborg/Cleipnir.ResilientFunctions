using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeaseUpdaters
{
    private readonly HashSet<StoredId> _activeUpdaters = new();
    private readonly System.Threading.Lock _lock = new();

    public bool Contains(StoredId flowId)
    {
        lock (_lock)
            return _activeUpdaters.Contains(flowId);
    }

    public void Add(StoredId flowId)
    {
        lock (_lock)
            _activeUpdaters.Add(flowId);
    }

    public void Remove(StoredId flowId)
    {
        lock (_lock)
            _activeUpdaters.Remove(flowId);
    }
}