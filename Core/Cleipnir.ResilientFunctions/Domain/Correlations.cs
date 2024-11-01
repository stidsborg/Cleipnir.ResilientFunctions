using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class Correlations(StoredId storedId, ICorrelationStore correlationStore)
{
    private HashSet<string>? _correlations;
    private readonly object _sync = new();

    private async Task<HashSet<string>> GetCorrelations()
    {
        lock (_sync)
            if (_correlations is not null)
                return _correlations;

        var correlations = (await correlationStore.GetCorrelations(storedId))
            .ToHashSet();
        
        lock (_sync)
            if (_correlations is null)
                return _correlations = correlations;
            else 
                return _correlations;
    }
    
    public async Task Register(string correlation)
    {
        var registered = await GetCorrelations();
        
        lock (_sync)
            if (registered.Contains(correlation))
                return;

        await correlationStore.SetCorrelation(storedId, correlation);
        
        lock (_sync)
            registered.Add(correlation);
    }

    public async Task<bool> Contains(string correlation)
    {
        var registered = await GetCorrelations();
        lock (_sync)
            return registered.Contains(correlation);
    }

    public async Task Remove(string correlation)
    {
        var registered = await GetCorrelations();
        
        lock (_sync)
            if (!registered.Contains(correlation))
                return;

        await correlationStore.RemoveCorrelation(storedId, correlation);

        lock (_sync)
            registered.Remove(correlation);
    }
}