using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryCorrelationStore : ICorrelationStore
{
    private readonly Dictionary<FunctionId, HashSet<string>> _correlations = new();
    private readonly Dictionary<string, HashSet<FunctionId>> _reverseLookup = new();
    private readonly object _sync = new();
    
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

    public Task SetCorrelation(FunctionId functionId, string correlationId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(functionId))
                _correlations[functionId] = new HashSet<string>();

            _correlations[functionId].Add(correlationId);

            if (!_reverseLookup.ContainsKey(correlationId))
                _reverseLookup[correlationId] = new HashSet<FunctionId>();

            _reverseLookup[correlationId].Add(functionId);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<FunctionId>> GetCorrelations(string correlationId)
    {
        lock (_sync)
        {
            if (!_reverseLookup.ContainsKey(correlationId))
                return new List<FunctionId>().CastTo<IReadOnlyList<FunctionId>>().ToTask();

            return _reverseLookup[correlationId].ToList().CastTo<IReadOnlyList<FunctionId>>().ToTask();
        }
    }

    public Task<IReadOnlyList<string>> GetCorrelations(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(functionId))
                return new List<string>().CastTo<IReadOnlyList<string>>().ToTask();

            return _correlations[functionId]
                .ToList()
                .CastTo<IReadOnlyList<string>>()
                .ToTask();
        }
    }

    public Task RemoveCorrelations(FunctionId functionId)
    {
        lock (_sync)
        {
            if (!_correlations.ContainsKey(functionId))
                return Task.CompletedTask;

            var correlations = _correlations[functionId];
            foreach (var correlation in correlations)
                _reverseLookup[correlation].Remove(functionId);

            _correlations.Remove(functionId);
        }

        return Task.CompletedTask;
    }

    public Task RemoveCorrelation(FunctionId functionId, string correlationId)
    {
        lock (_sync)
        {
            _correlations[functionId].Remove(correlationId);
            _reverseLookup[correlationId].Remove(functionId);
        }

        return Task.CompletedTask;
    }
}