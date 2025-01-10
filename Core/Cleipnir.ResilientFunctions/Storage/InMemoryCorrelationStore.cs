using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryCorrelationStore : ICorrelationStore
{
    private readonly Dictionary<StoredId, HashSet<string>> _correlations = new();
    private readonly Dictionary<string, HashSet<StoredId>> _reverseLookup = new();
    private readonly Lock _sync = new();
    
    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_sync)
        {
            _correlations.Clear();
            _reverseLookup.Clear();
        }

        return Task.CompletedTask;
    }

    public Task SetCorrelation(StoredId storedId, string correlationId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(storedId))
                _correlations[storedId] = new HashSet<string>();

            _correlations[storedId].Add(correlationId);

            if (!_reverseLookup.ContainsKey(correlationId))
                _reverseLookup[correlationId] = new HashSet<StoredId>();

            _reverseLookup[correlationId].Add(storedId);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<StoredId>> GetCorrelations(string correlationId)
    {
        lock (_sync)
        {
            if (!_reverseLookup.ContainsKey(correlationId))
                return new List<StoredId>().CastTo<IReadOnlyList<StoredId>>().ToTask();

            return _reverseLookup[correlationId].ToList().CastTo<IReadOnlyList<StoredId>>().ToTask();
        }
    }

    public Task<IReadOnlyList<StoredInstance>> GetCorrelations(StoredType flowType, string correlationId)
    {
        lock (_sync)
        {
            return _correlations
                .Where(kv => kv.Key.Type == flowType && kv.Value.Contains(correlationId))
                .Select(kv => kv.Key.Instance)
                .ToList()
                .CastTo<IReadOnlyList<StoredInstance>>()
                .ToTask();
        }
    }

    public Task<IReadOnlyList<string>> GetCorrelations(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(storedId))
                return new List<string>().CastTo<IReadOnlyList<string>>().ToTask();

            return _correlations[storedId]
                .ToList()
                .CastTo<IReadOnlyList<string>>()
                .ToTask();
        }
    }

    public Task RemoveCorrelations(StoredId storedId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(storedId))
                return Task.CompletedTask;

            var correlations = _correlations[storedId];
            foreach (var correlation in correlations)
                _reverseLookup[correlation].Remove(storedId);

            _correlations.Remove(storedId);
        }

        return Task.CompletedTask;
    }

    public Task RemoveCorrelation(StoredId storedId, string correlationId)
    {
        lock (_sync)
        {
            _correlations[storedId].Remove(correlationId);
            _reverseLookup[correlationId].Remove(storedId);
        }

        return Task.CompletedTask;
    }
}