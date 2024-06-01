using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class Correlations
{
    private readonly FunctionId _functionId;
    private readonly HashSet<string> _registered;
    private readonly ICorrelationStore _correlationStore;
    private readonly object _sync = new();

    public Correlations(
        FunctionId functionId,
        IEnumerable<string> existingCorrelations,
        ICorrelationStore correlationStore
        )
    {
        _functionId = functionId;
        _registered = existingCorrelations.ToHashSet();
        _correlationStore = correlationStore;
    }
    
    public async Task Register(string correlation)
    {
        lock (_sync)
            if (_registered.Contains(correlation))
                return;

        await _correlationStore.SetCorrelation(_functionId, correlation);
        
        lock (_sync)
            _registered.Add(correlation);
    }

    public bool Contains(string correlation)
    {
        lock (_sync)
            return _registered.Contains(correlation);
    }

    public async Task Remove(string correlation)
    {
        lock (_sync)
            if (!_registered.Contains(correlation))
                return;

        await _correlationStore.RemoveCorrelation(_functionId, correlation);

        lock (_sync)
            _registered.Remove(correlation);
    }
}